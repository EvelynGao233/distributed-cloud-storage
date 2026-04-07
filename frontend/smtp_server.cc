#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <pthread.h>
#include <stdbool.h>
#include <string.h>
#include <netinet/in.h>
#include <errno.h>
#include <stdint.h>
#include <signal.h>
#include <ctype.h>
#include <time.h>
#include <iostream>
#include <string>
#include <vector>
#include <sstream>
#include <thread>
#include <chrono>
#include <grpcpp/grpcpp.h>
#include "kv_client.h"
#include "smtp_server.h"
#include "myproto/coordinator.grpc.pb.h"

extern std::string generate_email_id();
extern std::string get_current_time();
extern std::vector<std::string> split(const std::string& str, char delimiter);

#define MAX_CONNECTIONS 100

static volatile sig_atomic_t smtp_shutting_down = 0;
static int smtp_active_connections[MAX_CONNECTIONS];
static pthread_mutex_t smtp_connections_mutex = PTHREAD_MUTEX_INITIALIZER;
static int smtp_connection_count = 0;
static int smtp_listen_fd = -1;
static KvStorageClient* smtp_kv_client = nullptr;
static bool verbose_mode = true;

// Parse email address from MAIL FROM:<> or RCPT TO:<> format
std::string parse_email_address(const char* command, int start_pos) {
    const char* start = strchr(command + start_pos, '<');
    const char* end = strchr(command + start_pos, '>');
    if (start && end && end > start) {
        return std::string(start + 1, end - start - 1);
    }
    return "";
}

// Extract username from email address
std::string extract_username(const std::string& email) {
    size_t at_pos = email.find('@');
    if (at_pos != std::string::npos) {
        return email.substr(0, at_pos);
    }
    return email;
}

// Check if user exists in the system
bool user_exists_in_kv(const std::string& username) {
    if (!smtp_kv_client) return false;

    std::string temp;
    return smtp_kv_client->Get(username + "-account", "password", temp);
}

// Store email in KV store
bool store_email_in_kv(const std::string& recipient_username, const std::string& sender_email,
                       const std::string& email_body) {
    if (!smtp_kv_client) {
        fprintf(stderr, "[SMTP] Error: KV client not initialized\n");
        return false;
    }

    try {
        // Extract subject from email body
        std::string subject = "(no subject)";
        std::istringstream iss(email_body);
        std::string line;
        bool in_headers = true;
        std::string body_content;

        while (std::getline(iss, line)) {
            if (!line.empty() && line.back() == '\r') {
                line.pop_back();
            }

            if (in_headers) {
                if (line.empty()) {
                    in_headers = false;
                    continue;
                }

                if (line.length() > 8 && strncasecmp(line.c_str(), "Subject:", 8) == 0) {
                    subject = line.substr(8);
                    size_t first = subject.find_first_not_of(" \t");
                    if (first != std::string::npos) {
                        subject = subject.substr(first);
                    }
                }
            } else {
                if (!body_content.empty()) {
                    body_content += "\n";
                }
                body_content += line;
            }
        }

        // If no body content was extracted, use the entire email body
        if (body_content.empty()) {
            body_content = email_body;
        }

        // Generate unique email ID and timestamp
        std::string email_id = generate_email_id();
        std::string timestamp = get_current_time();

        // Format: sender|subject|body|timestamp
        std::string email_data = sender_email + "|" + subject + "|" + body_content + "|" + timestamp;

        smtp_kv_client->Put(recipient_username + "-mailbox", email_id, email_data);

        // Update inbox list with retry loop for CPUT
        bool inbox_updated = false;
        int max_retries = 5;
        for (int attempt = 0; attempt < max_retries && !inbox_updated; attempt++) {
            std::string inbox_list;
            smtp_kv_client->Get(recipient_username + "-mailbox", "inbox", inbox_list);
            std::string new_inbox_list = inbox_list.empty() ? email_id : inbox_list + "," + email_id;

            if (smtp_kv_client->Cput(recipient_username + "-mailbox", "inbox", inbox_list, new_inbox_list)) {
                inbox_updated = true;
            }
        }

        if (!inbox_updated) {
            fprintf(stderr, "[SMTP] Warning: Failed to update inbox after retries for user %s\n", recipient_username.c_str());
        }

        fprintf(stderr, "[SMTP] Stored email %s for user %s (subject: %s)\n",
                email_id.c_str(), recipient_username.c_str(), subject.c_str());

        return true;
    } catch (const std::exception& e) {
        fprintf(stderr, "[SMTP] Exception storing email: %s\n", e.what());
        return false;
    }
}

void* handle_smtp_connection(void* arg) {
    int socket_fd = (int)(intptr_t)arg;
    char buffer[2000];
    int accum_len = 0;

    if (verbose_mode) {
        fprintf(stderr, "[SMTP %d] New connection\n", socket_fd);
    }

    const char* greeting_msg = "220 PennCloud SMTP Service ready\r\n";
    write(socket_fd, greeting_msg, strlen(greeting_msg));
    if (verbose_mode) {
        fprintf(stderr, "[SMTP %d] S: 220 PennCloud SMTP Service ready\n", socket_fd);
    }

    // SMTP state machine
    enum State {
        INITIAL,
        MAIL_RECEIVED,
        RCPT_RECEIVED
    };

    State state = INITIAL;
    bool helo_received = false;
    std::string sender_email;
    std::vector<std::string> recipients;

    bool quit_received = false;
    while (!quit_received && !smtp_shutting_down) {
        char read_buffer[1000];

        int bytes_read = read(socket_fd, read_buffer, sizeof(read_buffer) - 1);
        if (bytes_read <= 0) {
            if (bytes_read < 0 && errno == EINTR) {
                continue;
            }
            break;
        }

        // Add to accumulator buffer
        if (accum_len + bytes_read < sizeof(buffer)) {
            memcpy(buffer + accum_len, read_buffer, bytes_read);
            accum_len += bytes_read;
            buffer[accum_len] = '\0';
        } else {
            const char* overflow_msg = "500 Command too long\r\n";
            write(socket_fd, overflow_msg, strlen(overflow_msg));
            break;
        }

        // Process complete commands
        while (true) {
            char* crlf_pos = strstr(buffer, "\r\n");
            if (!crlf_pos) break;

            int cmd_len = crlf_pos - buffer;
            char cmd[1000];
            strncpy(cmd, buffer, cmd_len);
            cmd[cmd_len] = '\0';

            if (verbose_mode) {
                fprintf(stderr, "[SMTP %d] C: %s\n", socket_fd, cmd);
            }

            // Convert to uppercase for case-insensitive comparison
            char cmd_upper[1000];
            strcpy(cmd_upper, cmd);
            for (int i = 0; i < strlen(cmd_upper); i++) {
                cmd_upper[i] = toupper(cmd_upper[i]);
            }

            char response[1000];

            // HELO command
            if (strncmp(cmd_upper, "HELO", 4) == 0 && (cmd_upper[4] == '\0' || cmd_upper[4] == ' ')) {
                if (!helo_received || state == INITIAL) {
                    if (strlen(cmd) > 5) {
                        helo_received = true;
                        state = INITIAL;
                        snprintf(response, sizeof(response), "250 PennCloud\r\n");
                    } else {
                        snprintf(response, sizeof(response), "501 Syntax error in parameters\r\n");
                    }
                } else {
                    snprintf(response, sizeof(response), "503 Bad sequence of commands\r\n");
                }
            }
            // EHLO command (respond with error to force simple SMTP)
            else if (strncmp(cmd_upper, "EHLO", 4) == 0) {
                snprintf(response, sizeof(response), "500 Syntax error, command unrecognized\r\n");
            }
            // MAIL FROM
            else if (strncmp(cmd_upper, "MAIL FROM:", 10) == 0) {
                if (!helo_received) {
                    snprintf(response, sizeof(response), "503 Bad sequence of commands\r\n");
                } else if (state != INITIAL) {
                    snprintf(response, sizeof(response), "503 Bad sequence of commands\r\n");
                } else {
                    sender_email = parse_email_address(cmd, 10);
                    if (!sender_email.empty()) {
                        state = MAIL_RECEIVED;
                        recipients.clear();
                        snprintf(response, sizeof(response), "250 OK\r\n");
                    } else {
                        snprintf(response, sizeof(response), "501 Syntax error in parameters\r\n");
                    }
                }
            }
            // RCPT TO
            else if (strncmp(cmd_upper, "RCPT TO:", 8) == 0) {
                if (state != MAIL_RECEIVED && state != RCPT_RECEIVED) {
                    snprintf(response, sizeof(response), "503 Bad sequence of commands\r\n");
                } else {
                    std::string recipient = parse_email_address(cmd, 8);
                    if (!recipient.empty()) {
                        // Extract username and check if user exists
                        std::string username = extract_username(recipient);

                        // Check if recipient is for localhost/penncloud
                        size_t at_pos = recipient.find('@');
                        if (at_pos != std::string::npos) {
                            std::string domain = recipient.substr(at_pos + 1);

                            if (domain == "localhost" || domain == "penncloud" ||
                                domain == "127.0.0.1" || domain.find("penncloud") != std::string::npos) {

                                // Check if user exists in our system
                                if (user_exists_in_kv(username)) {
                                    recipients.push_back(recipient);
                                    state = RCPT_RECEIVED;
                                    snprintf(response, sizeof(response), "250 OK\r\n");
                                } else {
                                    snprintf(response, sizeof(response), "550 No such user here\r\n");
                                }
                            } else {
                                snprintf(response, sizeof(response), "550 Relay not permitted\r\n");
                            }
                        } else {
                            snprintf(response, sizeof(response), "550 No such user here\r\n");
                        }
                    } else {
                        snprintf(response, sizeof(response), "501 Syntax error in parameters\r\n");
                    }
                }
            }
            // DATA command
            else if (strncmp(cmd_upper, "DATA", 4) == 0 && cmd_upper[4] == '\0') {
                if (state != RCPT_RECEIVED) {
                    snprintf(response, sizeof(response), "503 Bad sequence of commands\r\n");
                } else {
                    snprintf(response, sizeof(response), "354 Start mail input; end with <CRLF>.<CRLF>\r\n");
                    write(socket_fd, response, strlen(response));
                    if (verbose_mode) {
                        fprintf(stderr, "[SMTP %d] S: 354 Start mail input; end with <CRLF>.<CRLF>\n", socket_fd);
                    }

                    // Read email data
                    std::string email_data;
                    accum_len = 0;
                    buffer[0] = '\0';
                    bool data_complete = false;

                    while (!data_complete && !smtp_shutting_down) {
                        char data_buffer[1000];
                        int bytes = read(socket_fd, data_buffer, sizeof(data_buffer) - 1);
                        if (bytes <= 0) {
                            if (bytes < 0 && errno == EINTR) continue;
                            break;
                        }
                        data_buffer[bytes] = '\0';
                        email_data += std::string(data_buffer, bytes);

                        // Check for end marker: \r\n.\r\n
                        size_t end_pos = email_data.find("\r\n.\r\n");
                        if (end_pos != std::string::npos) {
                            email_data = email_data.substr(0, end_pos + 2); // Keep the final \r\n
                            data_complete = true;
                        }
                    }

                    if (!data_complete) {
                        // Connection error
                        break;
                    }

                    // Store email for each recipient
                    bool all_success = true;
                    for (const auto& recipient : recipients) {
                        std::string username = extract_username(recipient);
                        if (!store_email_in_kv(username, sender_email, email_data)) {
                            all_success = false;
                        }
                    }

                    if (all_success) {
                        snprintf(response, sizeof(response), "250 OK\r\n");
                    } else {
                        snprintf(response, sizeof(response), "451 Local error in processing\r\n");
                    }

                    write(socket_fd, response, strlen(response));
                    if (verbose_mode) {
                        fprintf(stderr, "[SMTP %d] S: %s", socket_fd, response);
                    }

                    // Reset state
                    state = INITIAL;
                    sender_email.clear();
                    recipients.clear();
                    accum_len = 0;
                    buffer[0] = '\0';
                    continue;
                }
            }
            // QUIT command
            else if (strncmp(cmd_upper, "QUIT", 4) == 0 && cmd_upper[4] == '\0') {
                snprintf(response, sizeof(response), "221 PennCloud closing connection\r\n");
                quit_received = true;
            }
            // RSET command
            else if (strncmp(cmd_upper, "RSET", 4) == 0 && cmd_upper[4] == '\0') {
                if (!helo_received) {
                    snprintf(response, sizeof(response), "503 Bad sequence of commands\r\n");
                } else {
                    state = INITIAL;
                    sender_email.clear();
                    recipients.clear();
                    snprintf(response, sizeof(response), "250 OK\r\n");
                }
            }
            // NOOP command
            else if (strncmp(cmd_upper, "NOOP", 4) == 0 && cmd_upper[4] == '\0') {
                if (!helo_received) {
                    snprintf(response, sizeof(response), "503 Bad sequence of commands\r\n");
                } else {
                    snprintf(response, sizeof(response), "250 OK\r\n");
                }
            }
            // Unknown command
            else {
                snprintf(response, sizeof(response), "500 Syntax error, command unrecognized\r\n");
            }

            // Send response
            write(socket_fd, response, strlen(response));
            if (verbose_mode) {
                fprintf(stderr, "[SMTP %d] S: %s", socket_fd, response);
            }

            if (quit_received) break;

            // Remove processed command from buffer
            int remaining_len = accum_len - (cmd_len + 2);
            if (remaining_len > 0) {
                memmove(buffer, crlf_pos + 2, remaining_len);
            }
            accum_len = remaining_len;
            buffer[accum_len] = '\0';
        }
    }

    if (verbose_mode) {
        fprintf(stderr, "[SMTP %d] Connection closed\n", socket_fd);
    }

    // Remove from active connections
    pthread_mutex_lock(&smtp_connections_mutex);
    for (int i = 0; i < smtp_connection_count; i++) {
        if (smtp_active_connections[i] == socket_fd) {
            for (int j = i; j < smtp_connection_count - 1; j++) {
                smtp_active_connections[j] = smtp_active_connections[j + 1];
            }
            smtp_connection_count--;
            break;
        }
    }
    pthread_mutex_unlock(&smtp_connections_mutex);

    close(socket_fd);
    return NULL;
}

void smtp_signal_handler(int signum) {
    if (signum == SIGINT || signum == SIGTERM) {
        smtp_shutting_down = 1;

        pthread_mutex_lock(&smtp_connections_mutex);
        for (int i = 0; i < smtp_connection_count; i++) {
            const char* shutdown_msg = "421 Server shutting down\r\n";
            write(smtp_active_connections[i], shutdown_msg, strlen(shutdown_msg));
            close(smtp_active_connections[i]);
        }
        pthread_mutex_unlock(&smtp_connections_mutex);

        if (smtp_listen_fd >= 0) {
            close(smtp_listen_fd);
        }
    }
}

void smtpHeartbeatLoop(const std::string& smtp_addr, const std::string& coordinator_addr) {
    auto channel = grpc::CreateChannel(coordinator_addr, grpc::InsecureChannelCredentials());
    auto coordinator_stub = kvstorage::Coordinator::NewStub(channel);

    while (!smtp_shutting_down) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));

        kvstorage::HeartbeatRequest req;
        req.mutable_node()->set_address(smtp_addr);
        kvstorage::HeartbeatResponse resp;
        grpc::ClientContext ctx;
        grpc::Status s = coordinator_stub->Heartbeat(&ctx, req, &resp);
        if (!s.ok()) {
            std::cerr << "[SMTP] Heartbeat to coordinator failed: " << s.error_message() << std::endl;
        }
    }
}

void start_smtp_server(int port, const std::string& router_address) {
    fprintf(stderr, "[SMTP] Starting SMTP server on port %d\n", port);

    // Initialize KV client
    smtp_kv_client = new KvStorageClient(router_address);
    fprintf(stderr, "[SMTP] Connected to KV Coordinator at %s\n", router_address.c_str());

    // Create socket
    smtp_listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (smtp_listen_fd < 0) {
        perror("[SMTP] socket creation failed");
        return;
    }

    // Enable port reuse
    int sock_opt = 1;
    if (setsockopt(smtp_listen_fd, SOL_SOCKET, SO_REUSEADDR, &sock_opt, sizeof(sock_opt)) < 0) {
        perror("[SMTP] setsockopt failed");
        close(smtp_listen_fd);
        return;
    }

    // Bind
    struct sockaddr_in servaddr;
    memset(&servaddr, 0, sizeof(servaddr));
    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = INADDR_ANY;
    servaddr.sin_port = htons(port);

    if (bind(smtp_listen_fd, (struct sockaddr*)&servaddr, sizeof(servaddr)) < 0) {
        perror("[SMTP] bind failed");
        close(smtp_listen_fd);
        return;
    }

    // Listen
    if (listen(smtp_listen_fd, 10) < 0) {
        perror("[SMTP] listen failed");
        close(smtp_listen_fd);
        return;
    }

    fprintf(stderr, "[SMTP] Server listening on port %d\n", port);

    // Accept connections
    while (!smtp_shutting_down) {
        struct sockaddr_in clientaddr;
        socklen_t clientaddrlen = sizeof(clientaddr);
        int comm_fd = accept(smtp_listen_fd, (struct sockaddr*)&clientaddr, &clientaddrlen);

        if (comm_fd < 0) {
            if (smtp_shutting_down) break;
            if (errno == EINTR) continue;
            perror("[SMTP] accept failed");
            continue;
        }

        // Add to active connections
        pthread_mutex_lock(&smtp_connections_mutex);
        if (smtp_connection_count < MAX_CONNECTIONS) {
            smtp_active_connections[smtp_connection_count++] = comm_fd;
        }
        pthread_mutex_unlock(&smtp_connections_mutex);

        // Create thread to handle connection
        pthread_t thread;
        if (pthread_create(&thread, NULL, handle_smtp_connection, (void*)(intptr_t)comm_fd) != 0) {
            perror("[SMTP] thread creation failed");
            pthread_mutex_lock(&smtp_connections_mutex);
            smtp_connection_count--;
            pthread_mutex_unlock(&smtp_connections_mutex);
            close(comm_fd);
            continue;
        }
        pthread_detach(thread);
    }

    close(smtp_listen_fd);
    fprintf(stderr, "[SMTP] Server stopped\n");
}

void stop_smtp_server() {
    smtp_shutting_down = 1;
    if (smtp_listen_fd >= 0) {
        shutdown(smtp_listen_fd, SHUT_RDWR);
        close(smtp_listen_fd);
    }
}


void usage(const char* prog_name) {
    fprintf(stderr, "Usage: %s [-p port] [-r router_address]\n", prog_name);
    fprintf(stderr, "  -p port            SMTP server port (default: 2525)\n");
    fprintf(stderr, "  -r router_address  KV router address (default: localhost:7000)\n");
    exit(1);
}

int main(int argc, char* argv[]) {
    int port = 2525; // Default SMTP port
    std::string router_address = "localhost:7000";

    int opt;
    while ((opt = getopt(argc, argv, "p:r:h")) != -1) {
        switch (opt) {
            case 'p':
                port = atoi(optarg);
                break;
            case 'r':
                router_address = optarg;
                break;
            case 'h':
            default:
                usage(argv[0]);
        }
    }

    fprintf(stderr, "===================================\n");
    fprintf(stderr, "PennCloud SMTP Server\n");
    fprintf(stderr, "===================================\n");
    fprintf(stderr, "Port: %d\n", port);
    fprintf(stderr, "KV Router: %s\n", router_address.c_str());
    fprintf(stderr, "===================================\n\n");

    std::string smtp_addr = "localhost:" + std::to_string(port);
    std::thread heartbeat_thread(smtpHeartbeatLoop, smtp_addr, router_address);
    heartbeat_thread.detach();
    fprintf(stderr, "[SMTP] Heartbeat thread started for %s\n", smtp_addr.c_str());

    // Start SMTP server
    start_smtp_server(port, router_address);

    return 0;
}
