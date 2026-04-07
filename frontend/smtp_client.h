#ifndef SMTP_CLIENT_H
#define SMTP_CLIENT_H

#include <string>
#include <vector>

class SMTPClient {
public:
    SMTPClient();
    ~SMTPClient();

    bool send_email(const std::string& from_email, const std::string& to_email, const std::string& subject, const std::string& body, std::string& error_msg);

private:

    std::vector<std::string> lookup_mx_records(const std::string& domain);

    int connect_to_smtp_server(const std::string& mx_host, int port);

    bool read_smtp_response(int sock_fd, const std::string& expected_code, std::string& response, int timeout_sec = 30);

    bool send_smtp_command(int sock_fd, const std::string& command, const std::string& expected_code, std::string& response);

    bool smtp_handshake(int sock_fd, const std::string& our_hostname, std::string& error_msg);

    bool smtp_send_mail(int sock_fd, const std::string& from, const std::string& to, const std::string& subject, const std::string& body, std::string& error_msg);

    std::string extract_domain(const std::string& email);

    bool is_temporary_error(const std::string& response);
};

#endif
