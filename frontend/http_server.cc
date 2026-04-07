#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <signal.h>
#include <time.h>
#include <map>
#include <string>
#include <fstream>
#include <iostream>
#include <sstream>
#include <vector>
#include <thread>
#include <chrono>
#include <atomic>

#include "accounts_webmail.h"
#include "kv_client.h"
#include "storage.h"
#include "admin_console.h"
#include "frontend_node_registration.h"



extern KvStorageClient *g_kv_client;
extern std::string get_cookie_value(const std::string &cookies, const std::string &name);
extern std::map<std::string, std::string> parse_form_data(const std::string &body);
extern std::vector<std::string> split(const std::string &str, char delimiter);
extern std::string url_encode(const std::string &str);
extern std::string url_decode(const std::string &str);
extern std::string get_query_param(const std::string &url, const std::string &param);
extern std::string get_post_param(const std::string &body, const std::string &param);

volatile int shutting_down = 0;
volatile int listen_fd = -1;

void handle_signal(int signal)
{
    shutting_down = 1;
    if (listen_fd >= 0)
    {
        close(listen_fd);
    }
    exit(0);
}

// get HTML file content
char *get_html(const char *filename)
{
    FILE *file = fopen(filename, "r");
    if (!file)
    {
        fprintf(stderr, "Error: Cannot open file %s\n", filename);
        return NULL;
    }
    // 64KB buffer
    char *content = (char *)malloc(65536);
    if (!content)
    {
        fclose(file);
        return NULL;
    }
    size_t bytes_read = fread(content, 1, 65535, file);
    content[bytes_read] = '\0';
    fclose(file);
    return content;
}

void send_response(int fd, const char *status, const char *content_type,
                   const char *body, const char *cookie = NULL,
                   bool keep_alive = true)
{
    char header[2048];
    int body_len = strlen(body);

    if (cookie)
    {
        snprintf(header, sizeof(header),
                 "HTTP/1.1 %s\r\n"
                 "Content-Type: %s\r\n"
                 "Content-Length: %d\r\n"
                 "Connection: %s\r\n"
                 "Set-Cookie: %s; Path=/\r\n"
                 "\r\n",
                 status, content_type, body_len,
                 keep_alive ? "keep-alive" : "close",
                 cookie);
    }
    else
    {
        snprintf(header, sizeof(header),
                 "HTTP/1.1 %s\r\n"
                 "Content-Type: %s\r\n"
                 "Content-Length: %d\r\n"
                 "Connection: %s\r\n"
                 "\r\n",
                 status, content_type, body_len,
                 keep_alive ? "keep-alive" : "close");
    }

    send(fd, header, strlen(header), 0);
    send(fd, body, body_len, 0);
}

// send HTTP redirect
void send_redirect(int fd, const char *location, const char *cookie = NULL)
{
    char response[512];
    // with cookie
    if (cookie)
    {
        snprintf(response, sizeof(response),
                 "HTTP/1.1 302 Found\r\n"
                 "Location: %s\r\n"
                 "Set-Cookie: %s; Path=/\r\n"
                 "Content-Length: 0\r\n"
                 "\r\n",
                 location, cookie);
    }
    // without cookie
    else
    {
        snprintf(response, sizeof(response),
                 "HTTP/1.1 302 Found\r\n"
                 "Location: %s\r\n"
                 "Content-Length: 0\r\n"
                 "\r\n",
                 location);
    }
    send(fd, response, strlen(response), 0);
}

// Send response using chunked transfer encoding (for large files)
void send_response_chunked(int fd, const char *status, const char *content_type,
                           const std::string &body, const char *filename = NULL)
{
    char header[2048];

    // Build header with Transfer-Encoding: chunked
    if (filename)
    {
        snprintf(header, sizeof(header),
                 "HTTP/1.1 %s\r\n"
                 "Content-Type: %s\r\n"
                 "Content-Disposition: attachment; filename=\"%s\"\r\n"
                 "Transfer-Encoding: chunked\r\n"
                 "\r\n",
                 status, content_type, filename);
    }
    else
    {
        snprintf(header, sizeof(header),
                 "HTTP/1.1 %s\r\n"
                 "Content-Type: %s\r\n"
                 "Transfer-Encoding: chunked\r\n"
                 "\r\n",
                 status, content_type);
    }

    // Send header
    send(fd, header, strlen(header), 0);

    // Send body in chunks
    const size_t chunk_size = 8192; // 8KB chunks
    size_t offset = 0;
    size_t total_size = body.size();

    while (offset < total_size)
    {
        size_t remaining = total_size - offset;
        size_t current_chunk = (remaining < chunk_size) ? remaining : chunk_size;

        // Send chunk size in hexadecimal
        char chunk_header[32];
        snprintf(chunk_header, sizeof(chunk_header), "%zx\r\n", current_chunk);
        send(fd, chunk_header, strlen(chunk_header), 0);

        // Send chunk data
        const char *chunk_data = body.data() + offset;
        size_t chunk_sent = 0;
        while (chunk_sent < current_chunk)
        {
            ssize_t sent = send(fd, chunk_data + chunk_sent, current_chunk - chunk_sent, 0);
            if (sent <= 0)
            {
                fprintf(stderr, "[Error] Failed to send chunk: sent=%zd, errno=%d\n", sent, errno);
                return;
            }
            chunk_sent += sent;
        }

        // Send chunk trailing CRLF
        send(fd, "\r\n", 2, 0);

        offset += current_chunk;
    }

    // Send final zero chunk to indicate end
    send(fd, "0\r\n\r\n", 5, 0);

    fprintf(stderr, "[Chunked] Sent %zu bytes total\n", total_size);
}

// check if user is logged in (binary-safe version)
std::string get_logged_in_user(const char *buffer, int buffer_len)
{
    // Find Cookie header using binary-safe search
    const char *cookie_start = nullptr;
    for (int i = 0; i < buffer_len - 8; i++)
    {
        if (strncmp(buffer + i, "Cookie: ", 8) == 0)
        {
            cookie_start = buffer + i + 8;
            break;
        }
    }

    if (!cookie_start)
    {
        return "";
    }

    // Find end of cookie line
    const char *cookie_end = cookie_start;
    while (cookie_end < buffer + buffer_len && *cookie_end != '\r' && *cookie_end != '\n')
    {
        cookie_end++;
    }

    std::string cookie_line(cookie_start, cookie_end - cookie_start);

    // get session token
    std::string session_token = get_cookie_value(cookie_line, "session");
    if (session_token.empty())
    {
        return "";
    }

    // Get username from KV store、
    std::string username;
    g_user_accounts.get_user_from_session(session_token, username);
    return username;
}

// Generate breadcrumb HTML
std::string generate_breadcrumb(const std::string &current_path)
{
    std::string breadcrumb_html;

    // Always start with root
    breadcrumb_html += "<li class='breadcrumb-item'><a href='/drive'>My Drive</a></li>";

    if (current_path != "/" && !current_path.empty())
    {
        std::string path_so_far = "/";
        std::istringstream path_stream(current_path);
        std::string segment;

        while (std::getline(path_stream, segment, '/'))
        {
            if (!segment.empty())
            {
                path_so_far += segment + "/";
                breadcrumb_html += "<li class='breadcrumb-item'>";
                breadcrumb_html += "<a href='/drive?path=" + url_encode(path_so_far) + "'>";
                breadcrumb_html += segment + "</a></li>";
            }
        }
    }

    return breadcrumb_html;
}

std::string render_drive(const std::string &username,
                         const std::string &current_path_input = "/")
{
    char *html = get_html("pages/drive.html");
    if (!html)
    {
        return "<html><body><h1>Error loading page</h1></body></html>";
    }

    std::string html_str(html);
    free(html);

    std::string current_path = current_path_input;
    if (current_path.empty())
    {
        current_path = "/";
    }

    std::string placeholder = "<!-- USERNAME_PLACEHOLDER -->";
    size_t pos = 0;
    while ((pos = html_str.find(placeholder, pos)) != std::string::npos)
    {
        html_str.replace(pos, placeholder.size(), username);
        pos += username.size();
    }

    placeholder = "<!-- CURRENT_PATH -->";
    pos = 0;
    while ((pos = html_str.find(placeholder, pos)) != std::string::npos)
    {
        html_str.replace(pos, placeholder.size(), current_path);
        pos += current_path.size();
    }

    std::string storage_path = current_path;
    if (storage_path != "/" && !storage_path.empty() && storage_path[0] == '/')
    {
        storage_path = storage_path.substr(1);
    }

    std::string breadcrumb_html = generate_breadcrumb(current_path);

    placeholder = "<!-- BREADCRUMB_PLACEHOLDER -->";
    pos = 0;
    while ((pos = html_str.find(placeholder, pos)) != std::string::npos)
    {
        html_str.replace(pos, placeholder.size(), breadcrumb_html);
        pos += breadcrumb_html.size();
    }

    std::vector<FileItem> items = list_directory(username, storage_path);

    std::string file_list_html;

    if (items.empty())
    {
        file_list_html =
            "<div class='list-group-item text-center py-5 text-muted'>"
            "  <p>📁 No files yet. Upload your first file!</p>"
            "</div>";
    }
    else
    {
        for (const auto &item : items)
        {
            // Folder
            if (item.is_folder)
            {

                std::string folder_path =
                    (current_path == "/" ? "/" + item.name + "/" : current_path + item.name + "/");

                file_list_html +=
                    "<div class='file-item d-flex justify-content-between'>"
                    "  <div>"
                    "    <a href='/drive?path=" +
                    url_encode(folder_path) + "'>📁 " + item.name + "</a>"
                                                                    "  </div>"
                                                                    "  <div class='file-actions'>"

                                                                    // DELETE
                                                                    "<form method='POST' action='/delete' class='d-inline'>"
                                                                    "<input type='hidden' name='path' value='" +
                    item.full_path + "'>"
                                     "<input type='hidden' name='item_type' value='folder'>"
                                     "<input type='hidden' name='current_folder' value='" +
                    current_path + "'>"
                                   "<button class='btn btn-sm btn-danger'>Delete</button></form>"

                                   // RENAME (GET → rename_page)
                                   "<form method='GET' action='/rename_page' class='d-inline ms-1'>"
                                   "<input type='hidden' name='path' value='" +
                    item.full_path + "'>"
                                     "<input type='hidden' name='item_type' value='folder'>"
                                     "<input type='hidden' name='current_folder' value='" +
                    current_path + "'>"
                                   "<button class='btn btn-sm btn-warning'>Rename</button></form>"

                                   // MOVE (GET → move_page)
                                   "<form method='GET' action='/move_page' class='d-inline ms-1'>"
                                   "<input type='hidden' name='path' value='" +
                    item.full_path + "'>"
                                     "<input type='hidden' name='item_type' value='folder'>"
                                     "<input type='hidden' name='current_folder' value='" +
                    current_path + "'>"
                                   "<button class='btn btn-sm btn-info'>Move</button></form>"

                                   "  </div>"

                                   "</div>";
            }

            // File
            else
            {
                char size_buf[64];
                if (item.size < 1024)
                    snprintf(size_buf, sizeof(size_buf), "%zu B", item.size);
                else if (item.size < 1024 * 1024)
                    snprintf(size_buf, sizeof(size_buf), "%.1f KB", item.size / 1024.0);
                else
                    snprintf(size_buf, sizeof(size_buf), "%.1f MB", item.size / (1024.0 * 1024.0));

                file_list_html +=
                    "<div class='file-item d-flex justify-content-between'>"
                    "  <div>"
                    "    📄 " + item.name + " <small class='text-muted'>" + size_buf + "</small>"
                    "  </div>"
                    "  <div class='file-actions'>"

                    // DOWNLOAD
                    "<a href='/download?file=" + url_encode(item.full_path) + "' "
                        "class='btn btn-sm btn-primary'>Download</a>"

                    // DELETE
                    "<form method='POST' action='/delete' class='d-inline ms-1'>"
                        "<input type='hidden' name='path' value='" + item.full_path + "'>"
                        "<input type='hidden' name='item_type' value='file'>"
                        "<input type='hidden' name='current_folder' value='" + current_path + "'>"
                        "<button class='btn btn-sm btn-danger'>Delete</button>"
                    "</form>"

                    // RENAME
                    "<form method='GET' action='/rename_page' class='d-inline ms-1'>"
                        "<input type='hidden' name='path' value='" + item.full_path + "'>"
                        "<input type='hidden' name='item_type' value='file'>"
                        "<input type='hidden' name='current_folder' value='" + current_path + "'>"
                        "<button class='btn btn-sm btn-warning'>Rename</button>"
                    "</form>"

                    // MOVE
                    "<form method='GET' action='/move_page' class='d-inline ms-1'>"
                        "<input type='hidden' name='path' value='" + item.full_path + "'>"
                        "<input type='hidden' name='item_type' value='file'>"
                        "<input type='hidden' name='current_folder' value='" + current_path + "'>"
                        "<button class='btn btn-sm btn-info'>Move</button>"
                    "</form>"

                    "  </div>"
                    "</div>";
            }

        }
    }

    placeholder = "<!-- FILE_LIST_PLACEHOLDER -->";
    pos = html_str.find(placeholder);
    if (pos != std::string::npos)
    {
        html_str.replace(pos, placeholder.size(), file_list_html);
    }

    return html_str;
}

// parse multipart/form-data to get filename and content
void upload_helper(const char *buffer, int buffer_len, std::string &filename, std::string &content)
{
    // find filename
    const char *filename_start = strstr(buffer, "filename=\"");
    if (!filename_start)
        return;
    filename_start += 10;
    const char *filename_end = strchr(filename_start, '"');
    if (!filename_end)
        return;
    filename = std::string(filename_start, filename_end - filename_start);

    // find file content (after \r\n\r\n following Content-Type or filename)
    const char *content_start = strstr(filename_end, "\r\n\r\n");
    if (!content_start)
        return;
    content_start += 4;

    // find boundary end marker
    const char *boundary_start = strstr(buffer, "boundary=");
    if (!boundary_start)
        return;

    // Extract boundary (handle both formats: boundary=---- and boundary="----")
    boundary_start += 9; // skip "boundary="

    // Skip quotes if present
    if (*boundary_start == '"')
        boundary_start++;

    const char *boundary_end = boundary_start;
    while (*boundary_end && *boundary_end != '\r' && *boundary_end != '\n' && *boundary_end != ' ' && *boundary_end != '"')
    {
        boundary_end++;
    }

    std::string boundary(boundary_start, boundary_end - boundary_start);
    std::string end_marker = "\r\n--" + boundary;

    // Find end of content using binary-safe search
    const char *buffer_end = buffer + buffer_len;
    const char *content_end = nullptr;

    // Search for boundary marker after content_start
    for (const char *p = content_start; p <= buffer_end - end_marker.length(); p++)
    {
        if (memcmp(p, end_marker.c_str(), end_marker.length()) == 0)
        {
            content_end = p;
            break;
        }
    }

    if (!content_end)
    {
        content_end = buffer_end;
    }

    size_t content_len = content_end - content_start;
    content.assign(content_start, content_len);

    fprintf(stderr, "[Upload] File: %s, Size: %zu bytes\n",
            filename.c_str(), content.size());
}

void send_head_response(int fd, const char *status, const char *content_type,
                        int content_length, bool keep_alive = true)
{
    char header[1024];
    snprintf(header, sizeof(header),
             "HTTP/1.1 %s\r\n"
             "Content-Type: %s\r\n"
             "Content-Length: %d\r\n"
             "Connection: %s\r\n"
             "\r\n",
             status, content_type, content_length,
             keep_alive ? "keep-alive" : "close");
    send(fd, header, strlen(header), 0);
}

void send_redirect_to_drive(int fd, const std::string &folder_path)
{
    std::string response = "HTTP/1.1 303 See Other\r\n";
    response += "Location: /drive?path=" + url_encode(folder_path) + "\r\n";
    response += "Content-Length: 0\r\n\r\n";
    send(fd, response.c_str(), response.length(), 0);
}

// Read a single CRLF-terminated line from the socket.
static bool read_line_from_socket(int fd, std::string &line)
{
    line.clear();
    char c;
    while (true)
    {
        ssize_t n = read(fd, &c, 1);
        if (n <= 0)
        {
            // error or connection closed
            return false;
        }
        line.push_back(c);
        // stop when see "\r\n"
        size_t len = line.size();
        if (len >= 2 && line[len - 2] == '\r' && line[len - 1] == '\n')
        {
            break;
        }
    }
    return true;
}

// Read a chunked-encoded HTTP request body and append it into out_body.
static bool read_chunked_body(int fd, int max_size, std::string &out_body, bool &too_large)
{
    out_body.clear();
    too_large = false;

    while (true)
    {
        // 1) Read chunk size line: "<hex-size>[;extensions]\r\n"
        std::string size_line;
        if (!read_line_from_socket(fd, size_line))
        {
            fprintf(stderr, "[Error] Failed to read chunk size line\n");
            return false;
        }

        // Strip CRLF
        while (!size_line.empty() &&
               (size_line.back() == '\r' || size_line.back() == '\n'))
        {
            size_line.pop_back();
        }

        if (size_line.empty())
        {
            fprintf(stderr, "[Error] Empty chunk size line\n");
            return false;
        }

        // Remove optional chunk extensions (after ';')
        size_t semi = size_line.find(';');
        if (semi != std::string::npos)
        {
            size_line = size_line.substr(0, semi);
        }

        // Parse hex size
        char *endptr = nullptr;
        long chunk_size = strtol(size_line.c_str(), &endptr, 16);
        if (endptr == size_line.c_str() || chunk_size < 0)
        {
            fprintf(stderr, "[Error] Invalid chunk size: %s\n", size_line.c_str());
            return false;
        }

        if (chunk_size == 0)
        {
            // Final chunk
            std::string trailer_line;
            if (!read_line_from_socket(fd, trailer_line))
            {
                return false;
            }
            while (!(trailer_line == "\r\n" || trailer_line == "\n" || trailer_line == "\r"))
            {
                if (!read_line_from_socket(fd, trailer_line))
                {
                    return false;
                }
            }
            break; // end of body
        }

        // 2) Read exactly chunk_size bytes of data
        std::string chunk;
        chunk.resize(chunk_size);
        size_t received = 0;
        while (received < (size_t)chunk_size)
        {
            ssize_t n = read(fd, &chunk[received], chunk_size - received);
            if (n <= 0)
            {
                fprintf(stderr, "[Error] Failed to read chunk data\n");
                return false;
            }
            received += n;
        }

        out_body.append(chunk);

        if ((int)out_body.size() > max_size)
        {
            too_large = true;
            return false;
        }

        // 3) Read the CRLF after this chunk
        std::string crlf;
        if (!read_line_from_socket(fd, crlf))
        {
            fprintf(stderr, "[Error] Failed to read CRLF after chunk\n");
            return false;
        }
    }

    fprintf(stderr, "[ChunkedUpload] Total body size: %zu bytes\n", out_body.size());
    return true;
}

void *handle_client(void *arg)
{
    int client_fd = *(int *)arg;
    free(arg);

    // Set socket timeout to 2 minutes
    struct timeval timeout;
    timeout.tv_sec = 120; // 2 minutes
    timeout.tv_usec = 0;
    if (setsockopt(client_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) < 0)
    {
        fprintf(stderr, "[Warning] Failed to set socket timeout\n");
    }

    bool keep_alive = true;
    while (keep_alive)
    {
        if (!should_frontend_accept_connections())
        {
            printf("[Worker] Server is shutdown, closing connection\n");
            close(client_fd);
            return NULL;
        }
        // peek at the header to find header length and detect chunked vs Content-Length.
        char peek_buffer[8192];
        int peek_bytes = recv(client_fd, peek_buffer, sizeof(peek_buffer) - 1, MSG_PEEK);

        if (!should_frontend_accept_connections())
        {
            printf("[Worker] Server is shutdown, closing connection\n");
            close(client_fd);
            return NULL;
        }
        
        if (peek_bytes <= 0)
        {
            if (errno == EWOULDBLOCK || errno == EAGAIN)
            {
                fprintf(stderr, "[Timeout] Connection timed out after inactivity\n");
            }
            close(client_fd);
            return NULL;
        }
        peek_buffer[peek_bytes] = '\0';

        // Find end of header: "\r\n\r\n"
        char *header_end_marker = strstr(peek_buffer, "\r\n\r\n");
        int header_length = 0;
        if (header_end_marker)
        {
            header_length = (header_end_marker - peek_buffer) + 4;
        }
        else
        {
            fprintf(stderr, "[Error] Invalid HTTP request: no header end\n");
            close(client_fd);
            return NULL;
        }

        // Detect chunked transfer encoding in request
        bool is_chunked = false;
        if (strstr(peek_buffer, "Transfer-Encoding: chunked") != NULL)
        {
            is_chunked = true;
        }

        const int MAX_UPLOAD_SIZE = 25 * 1024 * 1024; // 25MB

        char *buffer = nullptr;
        int total_read = 0;
        int content_length = 0; // only used for non-chunked

        if (!is_chunked)
        {
            // Non-chunked path: use Content-Length
            char *cl_header = strstr(peek_buffer, "Content-Length:");
            if (cl_header)
            {
                sscanf(cl_header, "Content-Length: %d", &content_length);
            }

            int total_size = header_length + content_length;

            fprintf(stderr, "[Debug] Header: %d bytes, Body: %d bytes, Total: %d bytes\n",
                    header_length, content_length, total_size);

            if (total_size > MAX_UPLOAD_SIZE)
            {
                fprintf(stderr, "[Error] Request too large: %d bytes (max: %d)\n",
                        total_size, MAX_UPLOAD_SIZE);

                const char *response =
                    "HTTP/1.1 413 Payload Too Large\r\n"
                    "Content-Type: text/html\r\n"
                    "Content-Length: 78\r\n"
                    "Connection: close\r\n"
                    "\r\n"
                    "<html><body><h1>413 - File Too Large</h1><p>Max size: 20MB</p></body></html>";
                send(client_fd, response, strlen(response), 0);
                close(client_fd);
                return NULL;
            }

            buffer = (char *)malloc(total_size + 1);
            if (!buffer)
            {
                fprintf(stderr, "[Error] Cannot allocate %d bytes\n", total_size);
                const char *response =
                    "HTTP/1.1 500 Internal Server Error\r\n"
                    "Content-Type: text/html\r\n"
                    "Content-Length: 50\r\n"
                    "Connection: close\r\n"
                    "\r\n"
                    "<html><body><h1>500 - Out of Memory</h1></body></html>";
                send(client_fd, response, strlen(response), 0);
                close(client_fd);
                return NULL;
            }

            // Read the full request
            total_read = 0;
            while (total_read < total_size)
            {
                int bytes_to_read = total_size - total_read;
                int n = read(client_fd, buffer + total_read, bytes_to_read);

                if (n <= 0)
                {
                    if (n < 0)
                    {
                        fprintf(stderr, "[Error] read() failed: %s\n", strerror(errno));
                    }
                    else
                    {
                        fprintf(stderr, "[Error] Connection closed by client\n");
                    }
                    free(buffer);
                    close(client_fd);
                    return NULL;
                }

                total_read += n;

                if (total_size > 1024 * 1024 && n > 0)
                {
                    fprintf(stderr, "[Progress] %d / %d bytes (%.1f%%)\r",
                            total_read, total_size, 100.0 * total_read / total_size);
                }
            }

            if (total_size > 1024 * 1024)
            {
                fprintf(stderr, "\n[Debug] Successfully read %d bytes\n", total_read);
            }
        }
        else
        {
            // Chunked path: read header first, then decode chunked body
            fprintf(stderr, "[Info] Detected chunked request body\n");

            // Read exact header_length bytes into header_buf
            char *header_buf = (char *)malloc(header_length + 1);
            if (!header_buf)
            {
                fprintf(stderr, "[Error] Cannot allocate header buffer (%d bytes)\n", header_length);
                const char *response =
                    "HTTP/1.1 500 Internal Server Error\r\n"
                    "Content-Type: text/html\r\n"
                    "Content-Length: 50\r\n"
                    "Connection: close\r\n"
                    "\r\n"
                    "<html><body><h1>500 - Out of Memory</h1></body></html>";
                send(client_fd, response, strlen(response), 0);
                close(client_fd);
                return NULL;
            }

            int header_read = 0;
            while (header_read < header_length)
            {
                int n = read(client_fd, header_buf + header_read, header_length - header_read);
                if (n <= 0)
                {
                    if (n < 0)
                    {
                        fprintf(stderr, "[Error] read() header failed: %s\n", strerror(errno));
                    }
                    else
                    {
                        fprintf(stderr, "[Error] Connection closed by client while reading header\n");
                    }
                    free(header_buf);
                    close(client_fd);
                    return NULL;
                }
                header_read += n;
            }
            header_buf[header_length] = '\0';

            // Read chunked body
            std::string chunked_body;
            bool too_large = false;
            if (!read_chunked_body(client_fd, MAX_UPLOAD_SIZE, chunked_body, too_large))
            {
                free(header_buf);

                if (too_large)
                {
                    fprintf(stderr, "[Error] Chunked body too large (> %d bytes)\n", MAX_UPLOAD_SIZE);
                    const char *response =
                        "HTTP/1.1 413 Payload Too Large\r\n"
                        "Content-Type: text/html\r\n"
                        "Content-Length: 78\r\n"
                        "Connection: close\r\n"
                        "\r\n"
                        "<html><body><h1>413 - File Too Large</h1><p>Max size: 20MB</p></body></html>";
                    send(client_fd, response, strlen(response), 0);
                }
                else
                {
                    fprintf(stderr, "[Error] Failed to read chunked request body\n");
                }

                close(client_fd);
                return NULL;
            }

            int body_size = (int)chunked_body.size();
            total_read = header_length + body_size;

            fprintf(stderr, "[Debug] Header: %d bytes, Body: %d bytes (chunked), Total: %d bytes\n",
                    header_length, body_size, total_read);

            // Allocate one contiguous buffer = header + body
            buffer = (char *)malloc(total_read + 1);
            if (!buffer)
            {
                fprintf(stderr, "[Error] Cannot allocate %d bytes for full request\n", total_read);
                free(header_buf);
                const char *response =
                    "HTTP/1.1 500 Internal Server Error\r\n"
                    "Content-Type: text/html\r\n"
                    "Content-Length: 50\r\n"
                    "Connection: close\r\n"
                    "\r\n"
                    "<html><body><h1>500 - Out of Memory</h1></body></html>";
                send(client_fd, response, strlen(response), 0);
                close(client_fd);
                return NULL;
            }

            memcpy(buffer, header_buf, header_length);
            memcpy(buffer + header_length, chunked_body.data(), body_size);
            buffer[total_read] = '\0';

            free(header_buf);
        }

        // Parse HTTP request line
        char method[16], path[256], version[16];
        sscanf(buffer, "%s %s %s", method, path, version);

        fprintf(stderr, "[Request] %s %s (%d bytes)\n", method, path, total_read);

        // Find body using binary-safe search
        char *body = nullptr;
        int body_length = 0;

        for (int i = 0; i < total_read - 3; i++)
        {
            if (buffer[i] == '\r' && buffer[i + 1] == '\n' &&
                buffer[i + 2] == '\r' && buffer[i + 3] == '\n')
            {
                body = buffer + i + 4;
                body_length = total_read - (i + 4);
                break;
            }
        }

        // Check if client wants to close connection
        if (strstr(buffer, "Connection: close"))
        {
            keep_alive = false;
        }

        // Handle requests

        // GET
        if (strcmp(method, "GET") == 0)
        {
            // login
            if (strcmp(path, "/") == 0 || strcmp(path, "/login") == 0)
            {
                // check if logged in
                std::string username = get_logged_in_user(buffer, total_read);

                if (!username.empty())
                {
                    // logged in -> inbox
                    send_redirect(client_fd, "/inbox");
                }
                else
                {
                    // -> login page
                    char *html = get_html("pages/login.html");
                    if (html)
                    {
                        send_response(client_fd, "200 OK", "text/html", html, NULL, keep_alive);
                        free(html);
                    }
                    else
                    {
                        send_response(client_fd, "500 Internal Server Error", "text/html",
                                      "<html><body><h1>Error loading page</h1></body></html>", NULL, keep_alive);
                    }
                }
            }
            // Register
            else if (strcmp(path, "/register") == 0)
            {
                char *html = get_html("pages/register.html");
                if (html)
                {
                    send_response(client_fd, "200 OK", "text/html", html, NULL, keep_alive);
                    free(html);
                }
                else
                {
                    send_response(client_fd, "500 Internal Server Error", "text/html",
                                  "<html><body><h1>Error loading page</h1></body></html>", NULL, keep_alive);
                }
            }
            // Inbox
            else if (strcmp(path, "/inbox") == 0)
            {
                std::string username = get_logged_in_user(buffer, total_read);
                if (username.empty())
                {
                    send_redirect(client_fd, "/login");
                }
                else
                {
                    std::string inbox_html = handle_inbox(username);
                    send_response(client_fd, "200 OK", "text/html", inbox_html.c_str(), NULL, keep_alive);
                }
            }
            // View email
            else if (strncmp(path, "/mail/view", 10) == 0)
            {
                std::string username = get_logged_in_user(buffer, total_read);
                if (username.empty())
                {
                    send_redirect(client_fd, "/login");
                }
                else
                {
                    std::string email_id = get_query_param(path, "id");
                    std::string email_html = handle_view_email(username, email_id);
                    send_response(client_fd, "200 OK", "text/html", email_html.c_str(), NULL, keep_alive);
                }
            }
            // Compose
            else if (strcmp(path, "/mail/compose") == 0 || strcmp(path, "/compose") == 0)
            {
                std::string username = get_logged_in_user(buffer, total_read);
                if (username.empty())
                {
                    send_redirect(client_fd, "/login");
                }
                else
                {
                    char *html = get_html("pages/compose.html");
                    if (html)
                    {
                        std::string html_str(html);
                        free(html);

                        // Replace USERNAME_PLACEHOLDER
                        size_t pos = html_str.find("<!-- USERNAME_PLACEHOLDER -->");
                        if (pos != std::string::npos)
                        {
                            html_str.replace(pos, 29, username);
                        }

                        send_response(client_fd, "200 OK", "text/html", html_str.c_str(), NULL, keep_alive);
                    }
                    else
                    {
                        send_response(client_fd, "500 Internal Server Error", "text/html",
                                    "<html><body><h1>500 Error</h1></body></html>", NULL, keep_alive);
                    }
                }
            }
            // Reply
            else if (strncmp(path, "/mail/reply", 11) == 0)
            {
                std::string username = get_logged_in_user(buffer, total_read);
                if (username.empty())
                {
                    send_redirect(client_fd, "/login");
                }
                else
                {
                    std::string email_id = get_query_param(path, "id");
                    std::string html = handle_reply_get(username, email_id);
                    send_response(client_fd, "200 OK", "text/html", html.c_str(), NULL, keep_alive);
                }
            }
            // Forward
            else if (strncmp(path, "/mail/forward", 13) == 0)
            {
                std::string username = get_logged_in_user(buffer, total_read);
                if (username.empty())
                {
                    send_redirect(client_fd, "/login");
                }
                else
                {
                    std::string email_id = get_query_param(path, "id");
                    std::string html = handle_forward_get(username, email_id);
                    send_response(client_fd, "200 OK", "text/html", html.c_str(), NULL, keep_alive);
                }
            }
            // Delete email (GET)
            else if (strncmp(path, "/mail/delete", 12) == 0)
            {
                std::string username = get_logged_in_user(buffer, total_read);
                if (username.empty())
                {
                    send_redirect(client_fd, "/login");
                }
                else
                {
                    send_redirect(client_fd, "/inbox");
                }
            }
            // Health check endpoint for load balancer
            else if (strcmp(path, "/health") == 0)
            {
                send_response(client_fd, "200 OK", "text/plain", "OK", NULL, keep_alive);
            }
            // Logout
            else if (strcmp(path, "/logout") == 0)
            {
                // Find Cookie header using binary-safe search
                const char* cookie_start = nullptr;
                for (int i = 0; i < total_read - 8; i++)
                {
                    if (strncmp(buffer + i, "Cookie: ", 8) == 0)
                    {
                        cookie_start = buffer + i + 8;
                        break;
                    }
                }

                if (cookie_start)
                {
                    // Find end of cookie line
                    const char* cookie_end = cookie_start;
                    while (cookie_end < buffer + total_read && *cookie_end != '\r' && *cookie_end != '\n')
                    {
                        cookie_end++;
                    }

                    std::string cookie_line(cookie_start, cookie_end - cookie_start);
                    std::string session_token = get_cookie_value(cookie_line, "session");

                    if (!session_token.empty())
                    {
                        // Remove session from KV store
                        g_user_accounts.logout_user(session_token);
                    }
                }

                // Clear cookie and redirect to login
                send_redirect(client_fd, "/login", "session=; Max-Age=0");
            }
            // Drive
            else if (strcmp(path, "/drive") == 0 || strncmp(path, "/drive?", 7) == 0)
            {
                std::string username = get_logged_in_user(buffer, total_read);
                if (username.empty())
                {
                    send_redirect(client_fd, "/login");
                }
                else
                {
                    // Get current folder from query parameter
                    std::string current_folder = get_query_param(path, "path");
                    if (current_folder.empty())
                        current_folder = "/";

                    std::string drive_html = render_drive(username, current_folder);
                    send_response(client_fd, "200 OK", "text/html", drive_html.c_str(), NULL, keep_alive);
                }
            }
            // Download
            else if (strncmp(path, "/download", 9) == 0)
            {
                std::string username = get_logged_in_user(buffer, total_read);
                if (username.empty())
                {
                    send_redirect(client_fd, "/login");
                }
                else
                {
                    std::string file_path = get_query_param(path, "file");
                    if (file_path.empty())
                    {
                        const char *body = "Missing file parameter";
                        send_response(client_fd, "400 Bad Request", "text/plain", body, NULL, keep_alive);
                    }
                    else
                    {
                        auto t1 = std::chrono::steady_clock::now();
                        std::string content = download_file(username, file_path);
                        auto t2 = std::chrono::steady_clock::now();

                        if (content.empty())
                        {
                            const char *body = "File not found";
                            send_response(client_fd, "404 Not Found", "text/plain", body, NULL, keep_alive);
                        }
                        else
                        {
                            std::string filename = file_path;
                            size_t last_slash = file_path.find_last_of('/');
                            if (last_slash != std::string::npos)
                            {
                                filename = file_path.substr(last_slash + 1);
                            }

                            

                            send_response_chunked(client_fd, "200 OK", "application/octet-stream", content, filename.c_str());
                            auto t3 = std::chrono::steady_clock::now();

                            double t_get  = std::chrono::duration<double>(t2 - t1).count();
                            double t_send = std::chrono::duration<double>(t3 - t2).count();

                            fprintf(stderr,
                                    "[Perf] download_file (Get) took %.3f s, send took %.3f s, size=%zu bytes\n",
                                    t_get, t_send, content.size());
                        }
                    }
                }
            }

            else if (strncmp(path, "/images/", 8) == 0)
            {
                // ex. pages/images/upenn_logo.png
                char filename[512];
                snprintf(filename, sizeof(filename), "pages%s", path);

                FILE *img_file = fopen(filename, "rb");
                if (img_file)
                {
                    fseek(img_file, 0, SEEK_END);
                    long file_size = ftell(img_file);
                    fseek(img_file, 0, SEEK_SET);
                    char *img_content = (char *)malloc(file_size);
                    fread(img_content, 1, file_size, img_file);
                    fclose(img_file);

                    char header[512];
                    snprintf(header, sizeof(header),
                             "HTTP/1.1 200 OK\r\n"
                             "Content-Type: image/png\r\n"
                             "Content-Length: %ld\r\n"
                             "Connection: %s\r\n"
                             "\r\n",
                             file_size, keep_alive ? "keep-alive" : "close");

                    send(client_fd, header, strlen(header), 0);
                    send(client_fd, img_content, file_size, 0);

                    free(img_content);
                }
                else
                {
                    // 404 Not Found
                    fprintf(stderr, "Image not found: %s\n", filename);
                    char *html = get_html("pages/404.html");
                    if (html)
                    {
                        send_response(client_fd, "404 Not Found", "text/html", html, NULL, keep_alive);
                        free(html);
                    }
                    else
                    {
                        send_response(client_fd, "404 Not Found", "text/html",
                                      "<html><body><h1>404 Not Found</h1></body></html>", NULL, keep_alive);
                    }
                }
            }
            else if (strncmp(path, "/rename_page", 12) == 0)
            {
                std::string username = get_logged_in_user(buffer, total_read);
                if (username.empty())
                {
                    send_redirect(client_fd, "/login");
                }
                else
                {
                    std::string old_path = get_query_param(path, "path");
                    std::string item_type = get_query_param(path, "item_type");
                    std::string current_folder = get_query_param(path, "current_folder");

                    char *raw = get_html("pages/rename_page.html");
                    if (!raw)
                    {
                        send_response(client_fd, "500 Internal Server Error",
                                      "text/html", "<h1>Error</h1>");
                    }
                    else
                    {
                        std::string html(raw);
                        free(raw);

                        std::string old_name = old_path;
                        if (!old_name.empty() && old_name.back() == '/' && old_name.size() > 1)
                        {
                            old_name.pop_back();
                        }
                        size_t slash = old_name.find_last_of('/');
                        if (slash != std::string::npos)
                        {
                            old_name = old_name.substr(slash + 1);
                        }

                        auto rep = [&](const std::string &p, const std::string &v)
                        {
                            size_t pos = 0;
                            while ((pos = html.find(p, pos)) != std::string::npos)
                            {
                                html.replace(pos, p.size(), v);
                                pos += v.size();
                            }
                        };

                        rep("<!-- USERNAME_PLACEHOLDER -->", username);
                        rep("<!-- OLD_PATH -->", old_path);
                        rep("<!-- ITEM_TYPE -->", item_type);
                        rep("<!-- CURRENT_FOLDER -->", current_folder);
                        rep("<!-- OLD_NAME -->", old_name);

                        send_response(client_fd, "200 OK", "text/html", html.c_str(), NULL, keep_alive);
                    }
                }
            }

            // Move dialog page
            else if (strncmp(path, "/move_page", 10) == 0)
            {
                std::string username = get_logged_in_user(buffer, total_read);
                if (username.empty())
                {
                    send_redirect(client_fd, "/login");
                }
                else
                {
                    std::string item_path      = get_query_param(path, "path");
                    std::string item_type      = get_query_param(path, "item_type");
                    std::string current_folder = get_query_param(path, "current_folder");

                    char *raw = get_html("pages/move_page.html");
                    if (!raw)
                    {
                        send_response(client_fd, "500 Internal Server Error",
                                    "text/html", "<h1>Error</h1>");
                    }
                    else
                    {
                        std::string html(raw);
                        free(raw);

                        std::string item_name = item_path;
                        if (!item_name.empty() && item_name.back() == '/' && item_name.size() > 1)
                        {
                            item_name.pop_back();
                        }
                        size_t slash = item_name.find_last_of('/');
                        if (slash != std::string::npos)
                        {
                            item_name = item_name.substr(slash + 1);
                        }

                        std::vector<FileItem> items = list_files(username);
                        std::string list_html;

                        list_html += "<label class='list-group-item'>"
                                    "<input type='radio' class='form-check-input me-2' "
                                    "name='dest_folder' value='/' required> / </label>";

                        for (auto &f : items)
                        {
                            if (!f.is_folder)
                                continue;

                            std::string folder_value = f.full_path;

                            if (item_type == "folder")
                            {
                                if (folder_value == item_path)
                                    continue;

                                if (!item_path.empty() &&
                                    folder_value.size() > item_path.size() &&
                                    folder_value.compare(0, item_path.size(), item_path) == 0)
                                {
                                    continue;
                                }
                            }

                            std::string display_name = "/" + f.name;

                            list_html += "<label class='list-group-item'>"
                                        "<input type='radio' class='form-check-input me-2' "
                                        "name='dest_folder' value='" + folder_value + "'> "
                                        + display_name + "</label>";
                        }

                        auto rep = [&](const std::string &p, const std::string &v)
                        {
                            size_t pos = 0;
                            while ((pos = html.find(p, pos)) != std::string::npos)
                            {
                                html.replace(pos, p.size(), v);
                                pos += v.size();
                            }
                        };

                        rep("<!-- USERNAME_PLACEHOLDER -->", username);
                        rep("<!-- ITEM_PATH -->",           item_path);
                        rep("<!-- ITEM_TYPE -->",          item_type);
                        rep("<!-- CURRENT_FOLDER -->",     current_folder);
                        rep("<!-- ITEM_NAME -->",          item_name);
                        rep("<!-- FOLDER_LIST -->",        list_html);

                        send_response(client_fd, "200 OK", "text/html", html.c_str(), NULL, keep_alive);
                    }
                }
            }
       else if (strcmp(path, "/admin") == 0 || strncmp(path, "/admin?", 7) == 0)
        {
            std::string username = get_logged_in_user(buffer, total_read);
            if (username.empty())
            {
                send_redirect(client_fd, "/login");
            }
            else
            {
                std::string selected_node;
                std::string message;
                
                if (strncmp(path, "/admin?", 7) == 0)
                {
                    selected_node = get_query_param(path, "node");
                    message = get_query_param(path, "msg");
                }
                
                std::string html = render_admin_page(username, selected_node, "", message);
                send_response(client_fd, "200 OK", "text/html",
                            html.c_str(), NULL, keep_alive);
            }
        }
            else if (strcmp(path, "/reset-password") == 0)
            {
                std::string username = get_logged_in_user(buffer, total_read);
                if (username.empty())
                {
                    send_redirect(client_fd, "/login");
                }
                else
                {
                    char *html = get_html("pages/reset_password.html");
                    if (html)
                    {
                        send_response(client_fd, "200 OK", "text/html", html, NULL, keep_alive);
                        free(html);
                    }
                    else
                    {
                        send_response(client_fd, "500 Internal Server Error", "text/html",
                                      "<html><body><h1>500 Error</h1></body></html>", NULL, keep_alive);
                    }
                }
            }

            // 404 Not Found
            else
            {
                char *html = get_html("pages/404.html");
                if (html)
                {
                    send_response(client_fd, "404 Not Found", "text/html", html, NULL, keep_alive);
                    free(html);
                }
                else
                {
                    send_response(client_fd, "404 Not Found", "text/html",
                                  "<html><body><h1>404 Not Found</h1></body></html>", NULL, keep_alive);
                }
            }
        }
        // POST
        else if (strcmp(method, "POST") == 0)
        {
            // Login
            if (strcmp(path, "/login") == 0)
            {
                if (body)
                {
                    std::string body_str(body, body_length);
                    std::string set_cookie;

                    std::string response_html = handle_login_post(body_str, set_cookie);

                    if (!set_cookie.empty())
                    {
                        // success -> inbox
                        send_redirect(client_fd, "/inbox", set_cookie.c_str());
                    }
                    else
                    {
                        send_response(client_fd, "200 OK", "text/html", response_html.c_str(), NULL, keep_alive);
                    }
                }
            }
            // Register
            else if (strcmp(path, "/register") == 0)
            {
                if (body)
                {
                    std::string body_str(body, body_length);

                    std::string response_html = handle_register_post(body_str);
                    send_response(client_fd, "200 OK", "text/html", response_html.c_str(), NULL, keep_alive);
                }
            }
            // Send Email
            else if (strcmp(path, "/mail/send") == 0)
            {
                std::string username = get_logged_in_user(buffer, total_read);
                if (username.empty())
                {
                    send_redirect(client_fd, "/login");
                }
                else
                {
                    std::string body_str(body, body_length);

                    std::string response_html = handle_send_email(username, body_str);
                    send_response(client_fd, "200 OK", "text/html", response_html.c_str(), NULL, keep_alive);
                }
            }
            // Delete Email
            else if (strcmp(path, "/mail/delete") == 0)
            {
                std::string username = get_logged_in_user(buffer, total_read);
                if (username.empty())
                {
                    send_redirect(client_fd, "/login");
                }
                else
                {
                    std::string body_str(body, body_length);
                    auto form = parse_form_data(body_str);
                    std::string email_id = form["email_id"];

                    std::string response_html = handle_delete_email(username, email_id);
                    send_response(client_fd, "200 OK", "text/html", response_html.c_str(), NULL, keep_alive);
                }
            }
            else if (strcmp(path, "/reset-password") == 0)
            {
                std::string username = get_logged_in_user(buffer, total_read);
                if (username.empty())
                {
                    send_redirect(client_fd, "/login");
                }
                else
                {
                    if (body)
                    {
                        std::string body_str(body, body_length);
                        std::string response_html = handle_reset_password_post(body_str, username);
                        send_response(client_fd, "200 OK", "text/html", response_html.c_str(), NULL, keep_alive);
                    }
                }
            }
            else if (strncmp(path, "/upload", 7) == 0)
            {
                std::string username = get_logged_in_user(buffer, total_read);
                if (username.empty())
                {
                    send_redirect(client_fd, "/login");
                }
                else
                {
                    std::string current_folder = get_query_param(path, "path");
                    if (current_folder.empty())
                        current_folder = "/";

                    std::string filename, file_content;
                    upload_helper(buffer, total_read, filename, file_content);

                    if (!filename.empty() && !file_content.empty())
                    {
                        std::string storage_prefix = current_folder;

                        if (storage_prefix.empty() || storage_prefix == "/")
                        {
                            storage_prefix.clear();
                        }
                        else
                        {
                            if (storage_prefix[0] == '/')
                            {
                                storage_prefix = storage_prefix.substr(1); // "/hw/" -> "hw/"
                            }
                            if (!storage_prefix.empty() && storage_prefix.back() != '/')
                            {
                                storage_prefix.push_back('/');
                            }
                        }
                        std::string full_path = storage_prefix + filename;

                        if (!upload_file(username, full_path, file_content))
                        {
                            std::cerr << "[HTTP] Failed to upload file: " << full_path << std::endl;
                        }
                    }
                    else
                    {
                        std::cerr << "[HTTP] Failed to parse upload data" << std::endl;
                    }

                    send_redirect_to_drive(client_fd, current_folder);
                }
            }
            else if (strcmp(path, "/create_folder") == 0)
            {
                std::string username = get_logged_in_user(buffer, total_read);
                if (username.empty())
                {
                    send_redirect(client_fd, "/login");
                }
                else
                {
                    std::string body_str(body, body_length);

                    std::string folder_name    = get_post_param(body_str.c_str(), "folder_name");
                    std::string current_folder = get_post_param(body_str.c_str(), "current_folder");


                    if (current_folder.empty())
                        current_folder = "/";

                    // Build full folder path
                    std::string storage_prefix = current_folder;
                    if (storage_prefix.empty() || storage_prefix == "/")
                    {
                        storage_prefix.clear();
                    }
                    else
                    {
                        if (storage_prefix[0] == '/')
                        {
                            storage_prefix = storage_prefix.substr(1);
                        }
                        if (!storage_prefix.empty() && storage_prefix.back() != '/')
                        {
                            storage_prefix.push_back('/');
                        }
                    }

                    std::string full_folder_path = storage_prefix + folder_name + "/";
                    create_folder(username, full_folder_path);

                    // Redirect back to current folder
                    send_redirect_to_drive(client_fd, current_folder);
                }
            }
            else if (strcmp(path, "/delete") == 0)
            {
                std::string username = get_logged_in_user(buffer, total_read);
                if (username.empty())
                {
                    send_redirect(client_fd, "/login");
                }
                else
                {
                    std::string body_str(body, body_length);

                    std::string item_path      = get_post_param(body_str.c_str(), "path");
                    std::string item_type      = get_post_param(body_str.c_str(), "item_type");
                    std::string current_folder = get_post_param(body_str.c_str(), "current_folder");

                    if (current_folder.empty())
                        current_folder = "/";

                    bool success;
                    if (item_type == "folder")
                    {
                        success = delete_folder(username, item_path);
                    }
                    else
                    {
                        success = delete_file(username, item_path);
                    }

                    if (!success)
                    {
                        std::cerr << "[HTTP] Failed to delete: " << item_path << std::endl;
                    }

                    // Redirect back to current folder
                    send_redirect_to_drive(client_fd, current_folder);
                }
            }
            else if (strcmp(path, "/delete_folder") == 0)
            {
                std::string username = get_logged_in_user(buffer, total_read);
                if (username.empty())
                {
                    send_redirect(client_fd, "/login");
                }
                else
                {
                    std::string username = get_logged_in_user(buffer, total_read);
                    if (username.empty())
                    {
                        send_redirect(client_fd, "/");
                    }
                    else
                    {
                        if (body)
                        {
                            std::string body_str(body, body_length);
                            auto form = parse_form_data(body_str);
                            std::string folder_path = form["folder_path"];

                            if (!folder_path.empty())
                            {
                                delete_folder(username, folder_path);
                            }
                        }
                        send_redirect_to_drive(client_fd, "/");
                    }
                }
            }
            else if (strcmp(path, "/rename") == 0)
            {
                std::string username = get_logged_in_user(buffer, total_read);
                if (username.empty())
                {
                    send_redirect(client_fd, "/login");
                }
                else
                {
                    std::string body_str(body, body_length);

                    std::string old_path       = get_post_param(body_str.c_str(), "old_path");
                    std::string new_name       = get_post_param(body_str.c_str(), "new_name");
                    std::string item_type      = get_post_param(body_str.c_str(), "item_type");
                    std::string current_folder = get_post_param(body_str.c_str(), "current_folder");

                    if (current_folder.empty())
                        current_folder = "/";

                    std::cout << "[DEBUG] rename old_path='" << old_path
                            << "' new_name='" << new_name
                            << "' item_type='" << item_type
                            << "' current_folder='" << current_folder << "'" << std::endl;

                    bool is_folder = (item_type == "folder");

                    std::string parent = old_path;
                    if (!parent.empty() && parent.back() == '/' && is_folder)
                    {
                        parent.pop_back();
                    }

                    size_t last_slash = parent.find_last_of('/');
                    std::string storage_prefix;
                    if (last_slash == std::string::npos)
                    {
                        storage_prefix = "";
                    }
                    else
                    {
                        storage_prefix = parent.substr(0, last_slash + 1);
                    }

                    if (!is_folder)
                    {
                        std::string base = old_path;
                        if (!base.empty() && base.back() == '/')
                            base.pop_back();

                        size_t slash_pos = base.find_last_of('/');
                        if (slash_pos != std::string::npos)
                            base = base.substr(slash_pos + 1);

                        std::string old_ext;
                        size_t dot_pos = base.find_last_of('.');
                        if (dot_pos != std::string::npos && dot_pos != 0)
                        {
                            old_ext = base.substr(dot_pos);
                        }

                        size_t new_dot = new_name.find_last_of('.');
                        bool new_has_ext = (new_dot != std::string::npos && new_dot != 0);

                        if (!new_has_ext && !old_ext.empty())
                        {
                            new_name += old_ext;
                        }
                    }

                    std::string new_path = storage_prefix + new_name;
                    if (is_folder)
                    {
                        if (new_path.empty() || new_path.back() != '/')
                            new_path.push_back('/');
                    }

                    bool ok = is_folder
                                ? rename_folder(username, old_path, new_path)
                                : rename_file(username, old_path, new_path);

                    if (!ok)
                    {
                        std::cerr << "[HTTP] Rename failed: " << old_path
                                << " -> " << new_path << std::endl;
                    }

                    send_redirect_to_drive(client_fd, current_folder);
                }
            }

            else if (strcmp(path, "/move") == 0)
            {
                std::string username = get_logged_in_user(buffer, total_read);
                if (username.empty())
                {
                    send_redirect(client_fd, "/login");
                }
                else
                {
                    std::string body_str(body, body_length);

                    std::string item_path      = get_post_param(body_str.c_str(), "item_path");
                    std::string item_type      = get_post_param(body_str.c_str(), "item_type");
                    std::string dest_folder    = get_post_param(body_str.c_str(), "dest_folder");
                    std::string current_folder = get_post_param(body_str.c_str(), "current_folder");

                    std::cout << "[DEBUG] move item_path='" << item_path
                            << "' dest_folder='" << dest_folder
                            << "' item_type='" << item_type
                            << "' current_folder='" << current_folder << "'" << std::endl;

                    bool ok = (item_type == "folder")
                                  ? move_folder(username, item_path, dest_folder)
                                  : move_file(username, item_path, dest_folder);

                    if (!ok)
                    {
                        std::cerr << "[HTTP] Move failed: " << item_path
                                  << " -> " << dest_folder << std::endl;
                    }

                    send_redirect_to_drive(client_fd, current_folder);
                }
            }
            else if (strcmp(path, "/admin_control") == 0)
            {
                std::string username = get_logged_in_user(buffer, total_read);
                if (username.empty())
                {
                    send_redirect(client_fd, "/login");
                }
                else
                {
                    std::string body_str(body, body_length);
                    std::string node = get_post_param(body_str.c_str(), "node");
                    std::string cmd  = get_post_param(body_str.c_str(), "cmd");

                    std::string result_msg = execute_admin_command(node, cmd);

                    std::string redirect_url = "/admin?node=" + url_encode(node) + 
                                            "&msg=" + url_encode(result_msg);
                    send_redirect(client_fd, redirect_url.c_str());
                }
            }

        }
        // HEAD request
        else if (strcmp(method, "HEAD") == 0)
        {
            // Handle HEAD the same as GET but without body
            if (strcmp(path, "/") == 0)
            {
                send_head_response(client_fd, "200 OK", "text/html", 1024);
            }
            else
            {
                send_head_response(client_fd, "404 Not Found", "text/html", 100);
            }
        }
        // Other methods
        else
        {
            send_response(client_fd, "405 Method Not Allowed", "text/html",
                          "<html><body><h1>405 Method Not Allowed</h1></body></html>", NULL, false);
            keep_alive = false; // Close connection after 405
        }

        // Cleanup buffer for this request
        free(buffer);

        if (total_read <= 0)
            break;
    }

    // Close connection
    close(client_fd);
    return NULL;
}

int main(int argc, char *argv[])
{
    srand(time(NULL));
    signal(SIGINT, handle_signal);

    int port = 8000;
    int grpc_port = -1;
    std::string coordinator_address = "localhost:5051";

    int opt;
    while ((opt = getopt(argc, argv, "p:g:b:")) != -1)
    {
        if (opt == 'p') {
            port = atoi(optarg);
        } else if (opt == 'g') {
            grpc_port = atoi(optarg);
        } else if (opt == 'b') {
            coordinator_address = optarg;
        }
    }
    
    if (grpc_port == -1) {
        grpc_port = port + 1000;
    }
    
    std::string my_grpc_address = "localhost:" + std::to_string(grpc_port);
    start_frontend_registration(my_grpc_address, coordinator_address);
    
    std::string grpc_listen_addr = "0.0.0.0:" + std::to_string(grpc_port);
    start_frontend_grpc_control_server(grpc_listen_addr);
    printf("gRPC control server listening on port %d...\n", grpc_port);

    printf("Connecting to Coordinator at %s...\n", coordinator_address.c_str());
    init_kv_client(coordinator_address);
    printf("KV Client connected via Coordinator routing!\n");
    init_storage(g_kv_client);

    printf("Starting web server on port %d...\n", port);

    struct sockaddr_in servaddr;
    bzero(&servaddr, sizeof(servaddr));
    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = INADDR_ANY;
    servaddr.sin_port = htons(port);

    auto setup_socket = [&]() -> bool {
        listen_fd = socket(PF_INET, SOCK_STREAM, 0);
        if (listen_fd < 0)
        {
            fprintf(stderr, "Cannot open socket.\n");
            return false;
        }

        int socket_opt = 1;
        if (setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &socket_opt, sizeof(socket_opt)) < 0)
        {
            fprintf(stderr, "setsockopt error.\n");
            close(listen_fd);
            return false;
        }

        if (bind(listen_fd, (struct sockaddr *)&servaddr, sizeof(servaddr)) < 0)
        {
            fprintf(stderr, "bind failed.\n");
            close(listen_fd);
            return false;
        }

        if (listen(listen_fd, 100) < 0)
        {
            fprintf(stderr, "listen failed.\n");
            close(listen_fd);
            return false;
        }
        
        return true;
    };

    if (!setup_socket())
    {
        exit(1);
    }

    printf("Server ready! HTTP: %d, gRPC: %d\n", port, grpc_port);

    while (!shutting_down)
    {
        if (!should_frontend_accept_connections())
        {
            if (listen_fd >= 0)
            {
                printf("[Server] Closing listen socket (shutdown mode)\n");
                close(listen_fd);
                listen_fd = -1;
            }
            
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            continue;
        }
        else if (listen_fd < 0)
        {
            printf("[Server] Reopening listen socket (restart mode)\n");
            if (!setup_socket())
            {
                fprintf(stderr, "Failed to reopen socket on restart\n");
                std::this_thread::sleep_for(std::chrono::milliseconds(1000));
                continue;
            }
            printf("[Server] Listen socket reopened successfully\n");
        }

        struct sockaddr_in clientaddr;
        socklen_t clientlen = sizeof(clientaddr);

        int client_fd = accept(listen_fd, (struct sockaddr *)&clientaddr, &clientlen);
        if (client_fd < 0)
        {
            if (shutting_down)
                break;
            continue;
        }

        pthread_t thread;
        int *fd_ptr = (int *)malloc(sizeof(int));
        *fd_ptr = client_fd;
        pthread_create(&thread, NULL, handle_client, fd_ptr);
        pthread_detach(thread);
    }
    
    printf("[Main] Exited main loop, shutting_down=%d\n", shutting_down);
    
    if (listen_fd >= 0)
    {
        close(listen_fd);
    }
    
    stop_frontend_grpc_control_server();
    stop_frontend_registration();

    printf("[Main] Cleanup complete, exiting\n");
    return 0;
}