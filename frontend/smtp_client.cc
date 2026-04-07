#include "smtp_client.h"
#include <iostream>
#include <sstream>
#include <cstring>
#include <algorithm>
#include <ctime>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <arpa/nameser.h>
#include <resolv.h>
#include <netdb.h>
#include <errno.h>

using namespace std;

SMTPClient::SMTPClient() {
    res_init();
}

SMTPClient::~SMTPClient() {
}

string SMTPClient::extract_domain(const string& email) {
    size_t at_pos = email.find('@');
    if (at_pos != string::npos && at_pos + 1 < email.length()) {
        return email.substr(at_pos + 1);
    }
    return "";
}

bool SMTPClient::is_temporary_error(const string& response) {
    if (response.length() >= 3) {
        return response[0] == '4';
    }
    return false;
}

vector<string> SMTPClient::lookup_mx_records(const string& domain) {
    vector<pair<int, string>> mx_records; // priority, hostname
    unsigned char answer[4096];

    cerr << "[SMTP Client] Looking up MX records for domain: " << domain << endl;

    // Query for MX records
    int answer_len = res_query(domain.c_str(), C_IN, T_MX, answer, sizeof(answer));

    if (answer_len < 0) {
        cerr << "[SMTP Client] No MX records found, using domain itself as fallback" << endl;
        return {domain};
    }

    // Parse DNS response
    ns_msg msg;
    if (ns_initparse(answer, answer_len, &msg) < 0) {
        cerr << "[SMTP Client] Failed to parse DNS response" << endl;
        return {domain};
    }

    int count = ns_msg_count(msg, ns_s_an);
    cerr << "[SMTP Client] Found " << count << " DNS answer records" << endl;

    for (int i = 0; i < count; i++) {
        ns_rr rr;
        if (ns_parserr(&msg, ns_s_an, i, &rr) < 0) {
            continue;
        }

        if (ns_rr_type(rr) == T_MX) {
            // MX record
            const unsigned char* rdata = ns_rr_rdata(rr);
            int priority = (rdata[0] << 8) | rdata[1];

            char mx_host[256];
            if (dn_expand(ns_msg_base(msg), ns_msg_end(msg),
                         rdata + 2, mx_host, sizeof(mx_host)) < 0) {
                continue;
            }

            mx_records.push_back({priority, string(mx_host)});
            cerr << "[SMTP Client] Found MX: " << mx_host << " (priority " << priority << ")" << endl;
        }
    }

    if (mx_records.empty()) {
        cerr << "[SMTP Client] No valid MX records, using domain as fallback" << endl;
        return {domain};
    }

    // Sort by priority
    sort(mx_records.begin(), mx_records.end());

    vector<string> result;
    for (const auto& rec : mx_records) {
        result.push_back(rec.second);
    }

    return result;
}

int SMTPClient::connect_to_smtp_server(const string& mx_host, int port) {
    cerr << "[SMTP Client] Connecting to " << mx_host << ":" << port << endl;

    // Resolve hostname
    struct hostent* host_entry = gethostbyname(mx_host.c_str());
    if (host_entry == nullptr) {
        cerr << "[SMTP Client] Failed to resolve hostname: " << mx_host << endl;
        return -1;
    }

    int sock_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (sock_fd < 0) {
        cerr << "[SMTP Client] Failed to create socket: " << strerror(errno) << endl;
        return -1;
    }

    // Set connection timeout and server address
    struct timeval timeout;
    timeout.tv_sec = 30;
    timeout.tv_usec = 0;
    setsockopt(sock_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    setsockopt(sock_fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));

    // Setup server address
    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    memcpy(&server_addr.sin_addr, host_entry->h_addr_list[0], host_entry->h_length);

    if (connect(sock_fd, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        cerr << "[SMTP Client] Connection failed: " << strerror(errno) << endl;
        close(sock_fd);
        return -1;
    }

    cerr << "[SMTP Client] Connected successfully" << endl;
    return sock_fd;
}

bool SMTPClient::read_smtp_response(int sock_fd, const string& expected_code,
                                    string& response, int timeout_sec) {
    char buffer[4096];
    memset(buffer, 0, sizeof(buffer));

    // Set timeout
    struct timeval timeout;
    timeout.tv_sec = timeout_sec;
    timeout.tv_usec = 0;
    setsockopt(sock_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));

    int bytes_read = recv(sock_fd, buffer, sizeof(buffer) - 1, 0);
    if (bytes_read <= 0) {
        response = "Failed to read response from server";
        cerr << "[SMTP Client] " << response << endl;
        return false;
    }

    buffer[bytes_read] = '\0';
    response = string(buffer);

    string log_response = response;
    while (!log_response.empty() && (log_response.back() == '\n' || log_response.back() == '\r')) {
        log_response.pop_back();
    }
    cerr << "[SMTP Client] S: " << log_response << endl;

    if (!expected_code.empty()) {
        if (response.length() < expected_code.length() ||
            response.substr(0, expected_code.length()) != expected_code) {
            return false;
        }
    }

    return true;
}

bool SMTPClient::send_smtp_command(int sock_fd, const string& command, const string& expected_code, string& response) {
    // Log command
    string log_cmd = command;
    while (!log_cmd.empty() && (log_cmd.back() == '\n' || log_cmd.back() == '\r')) {
        log_cmd.pop_back();
    }
    cerr << "[SMTP Client] C: " << log_cmd << endl;

    // Send
    if (send(sock_fd, command.c_str(), command.length(), 0) < 0) {
        response = "Failed to send command to server";
        cerr << "[SMTP Client] " << response << endl;
        return false;
    }

    // Read
    return read_smtp_response(sock_fd, expected_code, response);
}

bool SMTPClient::smtp_handshake(int sock_fd, const string& our_hostname,
                                string& error_msg) {
    string response;

    // Read greeting (220)
    if (!read_smtp_response(sock_fd, "220", response, 60)) {
        error_msg = "Invalid greeting from server: " + response;
        return false;
    }

    // Try EHLO first, fall back to HELO if it fails
    string helo_cmd = "EHLO " + our_hostname + "\r\n";
    if (!send_smtp_command(sock_fd, helo_cmd, "", response)) {
        error_msg = "Failed to send EHLO command";
        return false;
    }

    // Check if EHLO succeeded (250) or if we need to use HELO
    if (response.length() < 3 || response.substr(0, 3) != "250") {
        cerr << "[SMTP Client] EHLO failed, trying HELO instead" << endl;
        helo_cmd = "HELO " + our_hostname + "\r\n";
        if (!send_smtp_command(sock_fd, helo_cmd, "250", response)) {
            error_msg = "HELO command failed: " + response;
            return false;
        }
    }

    return true;
}

bool SMTPClient::smtp_send_mail(int sock_fd, const string& from, const string& to, const string& subject, const string& body, string& error_msg) {
    string response;

    // MAIL FROM
    string mail_from_cmd = "MAIL FROM:<" + from + ">\r\n";
    if (!send_smtp_command(sock_fd, mail_from_cmd, "250", response)) {
        error_msg = "MAIL FROM failed: " + response;
        return false;
    }

    // RCPT TO
    string rcpt_to_cmd = "RCPT TO:<" + to + ">\r\n";
    if (!send_smtp_command(sock_fd, rcpt_to_cmd, "250", response)) {
        error_msg = "RCPT TO failed: " + response;
        return false;
    }

    // DATA
    if (!send_smtp_command(sock_fd, "DATA\r\n", "354", response)) {
        error_msg = "DATA command failed: " + response;
        return false;
    }

    // Send email with RFC 5322 header
    stringstream email_content;

    // unique Message-ID
    time_t now = time(nullptr);
    stringstream message_id;
    message_id << "<" << now << "." << getpid() << "@penncloud.com>";

    // current date
    char date_buf[128];
    struct tm* tm_info = gmtime(&now);
    strftime(date_buf, sizeof(date_buf), "%a, %d %b %Y %H:%M:%S +0000", tm_info);

    // generate RFC 5322 headers
    email_content << "From: " << from << "\r\n";
    email_content << "To: " << to << "\r\n";
    email_content << "Subject: " << subject << "\r\n";
    email_content << "Date: " << date_buf << "\r\n";
    email_content << "Message-ID: " << message_id.str() << "\r\n";
    email_content << "MIME-Version: 1.0\r\n";
    email_content << "Content-Type: text/plain; charset=UTF-8\r\n";
    email_content << "\r\n";
    email_content << body << "\r\n";
    email_content << ".\r\n";

    string email_str = email_content.str();
    cerr << "[SMTP Client] Sending email content (" << email_str.length() << " bytes)" << endl;

    if (!send_smtp_command(sock_fd, email_str, "250", response)) {
        error_msg = "Failed to send email content: " + response;
        return false;
    }

    // QUIT
    send_smtp_command(sock_fd, "QUIT\r\n", "221", response);

    return true;
}

bool SMTPClient::send_email(const string& from_email, const string& to_email, const string& subject, const string& body, string& error_msg) {
    cerr << "\n[SMTP Client] ======================================" << endl;
    cerr << "[SMTP Client] Sending email from " << from_email << " to " << to_email << endl;
    cerr << "[SMTP Client] Subject: " << subject << endl;
    cerr << "[SMTP Client] ======================================\n" << endl;

    // extract domain from sender email
    // Use the same domain name in HELO and MAIL FROM
    // this may help overcome spam filter??
    string sender_domain = extract_domain(from_email);
    if (sender_domain.empty()) {
        sender_domain = "penncloud.com";
    }

    // extract domain from recipient email
    string domain = extract_domain(to_email);
    if (domain.empty()) {
        error_msg = "Invalid recipient email format";
        cerr << "[SMTP Client] " << error_msg << endl;
        return false;
    }

    // Lookup MX records
    vector<string> mx_hosts = lookup_mx_records(domain);
    if (mx_hosts.empty()) {
        error_msg = "No MX records found for domain: " + domain;
        cerr << "[SMTP Client] " << error_msg << endl;
        return false;
    }

    // Try each MX host in order
    for (const auto& mx_host : mx_hosts) {
        cerr << "\n[SMTP Client] Trying MX host: " << mx_host << endl;

        int max_attempts = 3;
        for (int attempt = 1; attempt <= max_attempts; attempt++) {
            cerr << "[SMTP Client] Attempt " << attempt << " of " << max_attempts << endl;

            // port 25 for standard SMTP
            int sock_fd = connect_to_smtp_server(mx_host, 25);
            if (sock_fd < 0) {
                cerr << "[SMTP Client] Failed to connect, trying next MX host..." << endl;
                break;
            }

            if (!smtp_handshake(sock_fd, sender_domain, error_msg)) {
                cerr << "[SMTP Client] Handshake failed: " << error_msg << endl;
                close(sock_fd);

                if (is_temporary_error(error_msg) && attempt < max_attempts) {
                    cerr << "[SMTP Client] Temporary error, retrying after delay..." << endl;
                    sleep(5 * attempt);
                    continue;
                }
                break;
            }

            // Send email
            if (smtp_send_mail(sock_fd, from_email, to_email, subject, body, error_msg)) {
                cerr << "[SMTP Client] Email sent successfully!" << endl;
                close(sock_fd);
                return true;
            }

            cerr << "[SMTP Client] Failed to send email: " << error_msg << endl;
            close(sock_fd);

            if (is_temporary_error(error_msg) && attempt < max_attempts) {
                cerr << "[SMTP Client] Temporary error (4xx), retrying after delay..." << endl;
                sleep(5 * attempt);
                continue;
            }

            break;
        }
    }

    // All MX hosts failed
    if (error_msg.empty()) {
        error_msg = "Failed to send email after trying all MX servers";
    }
    cerr << "[SMTP Client] Final failure: " << error_msg << endl;
    return false;
}
