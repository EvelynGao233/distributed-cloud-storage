#ifndef ACCOUNTS_WEBMAIL_H
#define ACCOUNTS_WEBMAIL_H

#include <string>
#include <map>

void init_kv_client(const std::string& router_address);
std::string handle_register_post(const std::string& body);

std::string handle_login_post(const std::string& body, std::string& set_cookie);

std::string handle_reset_password_post(const std::string& body, const std::string& username);

std::string handle_inbox(const std::string& username);

std::string handle_view_email(const std::string& username, const std::string& email_id);

std::string handle_send_email(const std::string& sender, const std::string& body);

std::string handle_reply_get(const std::string& username, const std::string& email_id);

std::string handle_forward_get(const std::string& username, const std::string& email_id);

std::string handle_delete_email(const std::string& username, const std::string& email_id);

class UserAccounts {
public:
    UserAccounts();
    ~UserAccounts();
    
    bool register_user(const std::string& username, const std::string& password,
                      const std::string& email);
    
    bool login_user(const std::string& username, const std::string& password,
                   std::string& session_token);
    
    bool get_user_from_session(const std::string& session_token, std::string& username);

    void logout_user(const std::string& session_token);

    bool user_exists(const std::string& username);

    bool reset_password(const std::string& username, const std::string& new_password);
};

extern UserAccounts g_user_accounts;

#endif