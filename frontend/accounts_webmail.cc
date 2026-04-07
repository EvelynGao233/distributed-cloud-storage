#include <iostream>
#include <string>
#include <map>
#include <vector>
#include <sstream>
#include "accounts_webmail.h"
#include "smtp_client.h"
#include <grpcpp/grpcpp.h>
#include "kv_client.h"

using namespace std;

// External utility functions from utils.cc
extern string md5_hash(const string& str);
extern string generate_token();
extern string generate_email_id();
extern map<string, string> parse_form_data(const string& body);
extern map<string, string> parse_query_params(const string& query);
extern string get_cookie_value(const string& cookies, const string& name);
extern string get_current_time();
extern vector<string> split(const string& str, char delimiter);
extern char* get_html(const char* filename);

KvStorageClient* g_kv_client = nullptr;

void init_kv_client(const std::string& coordinator_address) {
    if (g_kv_client == nullptr) {
        g_kv_client = new KvStorageClient(coordinator_address);

        std::cout << "[Frontend] Connected to Coordinator at "
                  << coordinator_address << std::endl;
    }
}


// UserAccounts implementation
UserAccounts::UserAccounts() {
    // No need for local session storage - all in KV store
}

UserAccounts::~UserAccounts() {
    // No cleanup needed
}

bool UserAccounts::register_user(const string& username, const string& password,
                                  const string& email) {
    // Check if username exists
    string temp;
    if (g_kv_client->Get(username + "-account", "password", temp)) {
        return false; // Username already exists
    }

    // Check if email already registered
    if (g_kv_client->Get("email-to-username", email, temp)) {
        return false; // Email already registered
    }

    // Store user data
    string hashed_pw = md5_hash(password);
    g_kv_client->Put(username + "-account", "password", hashed_pw);
    g_kv_client->Put(username + "-account", "email", email);
    g_kv_client->Put(username + "-account", "created", get_current_time());

    // Store email to username mapping
    g_kv_client->Put("email-to-username", email, username);

    // Initialize empty inbox
    g_kv_client->Put(username + "-mailbox", "inbox", "");

    return true;
}

bool UserAccounts::login_user(const string& email, const string& password,
                               string& session_token) {
    // Find username from email
    string username;
    if (!g_kv_client->Get("email-to-username", email, username)) {
        return false; // Email not registered
    }

    // Verify password
    string stored_hash;
    if (!g_kv_client->Get(username + "-account", "password", stored_hash)) {
        return false; // User doesn't exist
    }

    string input_hash = md5_hash(password);
    if (stored_hash != input_hash) {
        return false; // Wrong password
    }

    // Create session
    session_token = generate_token();
    g_kv_client->Put("sessions", session_token, username);
    
    cout << "[Session] Created session " << session_token << " for user " << username << endl;

    return true;
}

bool UserAccounts::get_user_from_session(const string& session_token, string& username) {
    // Retrieve session from KV store
    bool found = g_kv_client->Get("sessions", session_token, username);
    
    if (found) {
        cout << "[Session] Found session " << session_token << " -> user " << username << endl;
    } else {
        cout << "[Session] Session " << session_token << " not found or expired" << endl;
    }
    
    return found;
}

void UserAccounts::logout_user(const string& session_token) {
    // Delete session from KV store
    g_kv_client->Delete("sessions", session_token);
    cout << "[Session] Deleted session " << session_token << endl;
}

bool UserAccounts::user_exists(const string& username) {
    string temp;
    return g_kv_client->Get(username + "-account", "password", temp);
}

bool UserAccounts::reset_password(const string& username, const string& new_password) {
    if (!user_exists(username)) {
        return false;
    }

    string hashed_pw = md5_hash(new_password);
    return g_kv_client->Put(username + "-account", "password", hashed_pw);
}

UserAccounts g_user_accounts;

string handle_register_post(const string& body) {
    auto form = parse_form_data(body);
    string username = form["username"];
    string password = form["password"];
    string confirm_password = form["confirm_password"];
    string email = username + "@penncloud.com";

    char* html = get_html("pages/register.html");
    if (!html) return "<html><body>Error loading page</body></html>";

    string html_str(html);
    free(html);

    string error_html = "";

    // Validation
    if (username.empty() || password.empty()) {
        error_html = "<div class=\"alert alert-danger\" role=\"alert\">All fields are required!</div>";
    } else if (password != confirm_password) {
        error_html = "<div class=\"alert alert-danger\" role=\"alert\">Passwords do not match!</div>";
    } else if (password.length() < 6) {
        error_html = "<div class=\"alert alert-danger\" role=\"alert\">Password must be at least 6 characters!</div>";
    } else if (g_user_accounts.register_user(username, password, email)) {
        error_html = "<div class=\"alert alert-success\" role=\"alert\">Registration successful! <a href='/login'>Click here to login</a></div>";
    } else {
        error_html = "<div class=\"alert alert-danger\" role=\"alert\">Username already exists!</div>";
    }

    // Replace placeholder
    size_t pos = html_str.find("<!-- ERROR_PLACEHOLDER -->");
    if (pos != string::npos) {
        html_str.replace(pos, 26, error_html);
    }

    return html_str;
}

string handle_login_post(const string& body, string& set_cookie) {
    auto form = parse_form_data(body);
    string email = form["email"];
    string password = form["password"];

    string session_token;
    if (g_user_accounts.login_user(email, password, session_token)) {
        set_cookie = "session=" + session_token + "; Path=/; HttpOnly";
        return ""; // redirect to /inbox
    } else {
        // Read login.html and insert error message
        char* html = get_html("pages/login.html");
        if (!html) return "<html><body>Error loading page</body></html>";

        string html_str(html);
        free(html);

        string error_html = "<div class=\"alert alert-danger\" role=\"alert\">Invalid email or password!</div>";

        // Replace <!-- ERROR_PLACEHOLDER -->
        size_t pos = html_str.find("<!-- ERROR_PLACEHOLDER -->");
        if (pos != string::npos) {
            html_str.replace(pos, 26, error_html);
        }

        return html_str;
    }
}

string handle_reset_password_post(const string& body, const string& username) {
    auto form = parse_form_data(body);
    string password = form["password"];
    string confirm_password = form["confirm_password"];

    char* html = get_html("pages/reset_password.html");
    if (!html) return "<html><body>Error loading page</body></html>";

    string html_str(html);
    free(html);

    string error_html = "";

    if (password.empty()) {
        error_html = "<div class=\"alert alert-danger\" role=\"alert\">All fields are required!</div>";
    } else if (password != confirm_password) {
        error_html = "<div class=\"alert alert-danger\" role=\"alert\">Passwords do not match!</div>";
    } else if (password.length() < 6) {
        error_html = "<div class=\"alert alert-danger\" role=\"alert\">Password must be at least 6 characters!</div>";
    } else if (g_user_accounts.reset_password(username, password)) {
        error_html = "<div class=\"alert alert-success\" role=\"alert\">Password changed successfully! <a href='/inbox'>Back to inbox</a></div>";
    } else {
        error_html = "<div class=\"alert alert-danger\" role=\"alert\">Failed to reset password!</div>";
    }

    size_t pos = html_str.find("<!-- ERROR_PLACEHOLDER -->");
    if (pos != string::npos) {
        html_str.replace(pos, 26, error_html);
    }

    return html_str;
}

string handle_inbox(const string& username) {
    char* html = get_html("pages/inbox.html");
    if (!html) return "<html><body>Error loading page</body></html>";

    string html_str(html);
    free(html);

    // Replace USERNAME_PLACEHOLDER with current username
    size_t pos = html_str.find("<!-- USERNAME_PLACEHOLDER -->");
    if (pos != string::npos) {
        html_str.replace(pos, 29, username);
    }

    // Get inbox emails from KV store
    string inbox_list;
    g_kv_client->Get(username + "-mailbox", "inbox", inbox_list);

    string email_list_html;

    if (inbox_list.empty()) {
        // Keep the default empty state from template
        return html_str;
    } else {
        // Generate email list
        auto email_ids = split(inbox_list, ',');
        email_list_html = "";

        for (const auto& email_id : email_ids) {
            string email_data;
            if (g_kv_client->Get(username + "-mailbox", email_id, email_data)) {
                auto parts = split(email_data, '|');
                if (parts.size() >= 4) {
                    email_list_html +=
                        "<a href='/mail/view?id=" + email_id + "' class='list-group-item list-group-item-action'>"
                        "  <div class='d-flex w-100 justify-content-between'>"
                        "    <h6 class='mb-1'><strong>" + parts[0] + "</strong></h6>"
                        "    <small class='text-muted'>" + parts[3] + "</small>"
                        "  </div>"
                        "  <p class='mb-1'>" + parts[1] + "</p>"
                        "</a>";
                }
            }
        }

        // Replace the entire empty state div with email list
        size_t start = html_str.find("<div class=\"list-group-item text-center py-5 text-muted\">");
        if (start != string::npos) {
            size_t end = html_str.find("</div>", start);
            end = html_str.find("</div>", end + 1);
            end = html_str.find("</div>", end + 1) + 6;
            html_str.replace(start, end - start, email_list_html);
        }
    }

    return html_str;
}

string handle_view_email(const string& username, const string& email_id) {
    char* html = get_html("pages/view_email.html");
    if (!html) return "<html><body>Error loading page</body></html>";

    string html_str(html);
    free(html);

    // Replace USERNAME_PLACEHOLDER
    size_t pos = html_str.find("<!-- USERNAME_PLACEHOLDER -->");
    if (pos != string::npos) {
        html_str.replace(pos, 29, username);
    }

    // Get email from KV store
    string email_data;
    if (!g_kv_client->Get(username + "-mailbox", email_id, email_data)) {
        return "<html><body><h1>Email not found</h1><a href='/inbox'>Back to inbox</a></body></html>";
    }

    auto parts = split(email_data, '|');
    if (parts.size() < 4) {
        return "<html><body><h1>Invalid email format</h1><a href='/inbox'>Back to inbox</a></body></html>";
    }

    // Replace all placeholders

    // FROM
    pos = html_str.find("<!-- FROM_PLACEHOLDER -->");
    if (pos != string::npos) {
        html_str.replace(pos, 25, parts[0]);
    }

    // TO
    pos = html_str.find("<!-- TO_PLACEHOLDER -->");
    if (pos != string::npos) {
        html_str.replace(pos, 23, username);
    }

    // SUBJECT
    pos = html_str.find("<!-- SUBJECT_PLACEHOLDER -->");
    if (pos != string::npos) {
        html_str.replace(pos, 28, parts[1]);
    }

    // DATE
    pos = html_str.find("<!-- DATE_PLACEHOLDER -->");
    if (pos != string::npos) {
        html_str.replace(pos, 25, parts[3]);
    }

    // BODY
    pos = html_str.find("<!-- BODY_PLACEHOLDER -->");
    if (pos != string::npos) {
        html_str.replace(pos, 25, parts[2]);
    }

    // EMAIL_ID (for reply/forward/delete buttons)
    while ((pos = html_str.find("<!-- EMAIL_ID_PLACEHOLDER -->")) != string::npos) {
        html_str.replace(pos, 29, email_id);
    }

    return html_str;
}

string handle_send_email(const string& sender, const string& body) {
    auto form = parse_form_data(body);
    string recipient_email = form["recipient"];
    string subject = form["subject"];
    string email_body = form["body"];

    if (recipient_email.empty() || subject.empty() || email_body.empty()) {
        return "<html><body><div class='alert alert-danger'>All fields are required!</div><a href='/compose'>Back</a></body></html>";
    }

    // Check if recipient is a local user or external
    string recipient_username;
    bool is_local_user = g_kv_client->Get("email-to-username", recipient_email, recipient_username);

    if (is_local_user) {
        // Local PennCloud user
        cout << "[Webmail] Sending local email to user: " << recipient_username << endl;

        string email_id = generate_email_id();
        string timestamp = get_current_time();

        // Get sender's email for display
        string sender_email;
        if (!g_kv_client->Get(sender + "-account", "email", sender_email)) {
            sender_email = sender + "@penncloud.com";
        }

        string email_data = sender_email + "|" + subject + "|" + email_body + "|" + timestamp;

        g_kv_client->Put(recipient_username + "-mailbox", email_id, email_data);

        // Update inbox list with retry loop for CPUT
        bool inbox_updated = false;
        int max_retries = 5;
        for (int attempt = 0; attempt < max_retries && !inbox_updated; attempt++) {
            string inbox_list;
            g_kv_client->Get(recipient_username + "-mailbox", "inbox", inbox_list);
            string new_inbox_list = inbox_list.empty() ? email_id : inbox_list + "," + email_id;

            if (g_kv_client->Cput(recipient_username + "-mailbox", "inbox", inbox_list, new_inbox_list)) {
                inbox_updated = true;
            }
        }

        if (!inbox_updated) {
            cerr << "[Warning] Failed to update inbox after retries for user: " << recipient_username << endl;
        }

        // Return success page with redirect
        return "<html><head><meta http-equiv='refresh' content='2;url=/inbox'></head><body><div class='alert alert-success'>Email sent successfully! Redirecting...</div></body></html>";
    }
    else {
        // External SMTP client
        cout << "[Webmail] Sending external email to: " << recipient_email << endl;

        string sender_email;
        if (!g_kv_client->Get(sender + "-account", "email", sender_email)) {
            sender_email = sender + "@penncloud.com";
        }

        SMTPClient smtp_client;
        string error_msg;

        if (smtp_client.send_email(sender_email, recipient_email, subject, email_body, error_msg)) {
            cout << "[Webmail] External email sent successfully!" << endl;
            return "<html><head><meta http-equiv='refresh' content='2;url=/inbox'></head>"
                   "<body><div class='alert alert-success'>"
                   "Email sent to external recipient successfully! Redirecting..."
                   "</div></body></html>";
        } else {
            cerr << "[Webmail] Failed to send external email: " << error_msg << endl;
            return "<html><body><div class='alert alert-danger'>"
                   "Failed to send external email: " + error_msg + "<br><br>";
                   
        }
    }
}

string handle_reply_get(const string& username, const string& email_id) {
    // Get original email
    string email_data;
    if (!g_kv_client->Get(username + "-mailbox", email_id, email_data)) {
        return "<html><body><h1>Email not found</h1><a href='/inbox'>Back to inbox</a></body></html>";
    }

    auto parts = split(email_data, '|');
    if (parts.size() < 4) {
        return "<html><body><h1>Invalid email format</h1><a href='/inbox'>Back to inbox</a></body></html>";
    }

    string from = parts[0];
    string subject = parts[1];
    string body = parts[2];

    // Load compose page
    char* html = get_html("pages/compose.html");
    if (!html) return "<html><body>Error loading page</body></html>";

    string html_str(html);
    free(html);

    // Replace USERNAME_PLACEHOLDER
    size_t pos = html_str.find("<!-- USERNAME_PLACEHOLDER -->");
    if (pos != string::npos) {
        html_str.replace(pos, 29, username);
    }

    // Pre-fill recipient with original sender
    pos = html_str.find("<input type=\"email\" name=\"recipient\" class=\"form-control\" placeholder=\"recipient@example.com\"");
    if (pos != string::npos) {
        html_str.replace(pos, 97,
            "<input type=\"email\" name=\"recipient\" class=\"form-control\" placeholder=\"recipient@example.com\" value=\"" + from + "\"");
    }

    // Pre-fill subject with "Re: "
    string reply_subject = subject.find("Re: ") == 0 ? subject : "Re: " + subject;
    pos = html_str.find("<input type=\"text\" name=\"subject\" class=\"form-control\" placeholder=\"Enter subject\"");
    if (pos != string::npos) {
        html_str.replace(pos, 84,
            "<input type=\"text\" name=\"subject\" class=\"form-control\" placeholder=\"Enter subject\" value=\"" + reply_subject + "\"");
    }

    // Pre-fill body with quoted original message
    string quoted_body = "\n\n--- Original Message ---\n" + body;
    pos = html_str.find("<textarea name=\"body\" class=\"form-control\" rows=\"10\"");
    if (pos != string::npos) {
        size_t end_pos = html_str.find("</textarea>", pos);
        if (end_pos != string::npos) {
            size_t start_content = html_str.find(">", pos) + 1;
            html_str.replace(start_content, end_pos - start_content, quoted_body);
        }
    }

    return html_str;
}

string handle_forward_get(const string& username, const string& email_id) {
    // Get original email
    string email_data;
    if (!g_kv_client->Get(username + "-mailbox", email_id, email_data)) {
        return "<html><body><h1>Email not found</h1><a href='/inbox'>Back to inbox</a></body></html>";
    }

    auto parts = split(email_data, '|');
    if (parts.size() < 4) {
        return "<html><body><h1>Invalid email format</h1><a href='/inbox'>Back to inbox</a></body></html>";
    }

    string from = parts[0];
    string subject = parts[1];
    string body = parts[2];

    // Load compose page
    char* html = get_html("pages/compose.html");
    if (!html) return "<html><body>Error loading page</body></html>";

    string html_str(html);
    free(html);

    // Replace USERNAME_PLACEHOLDER
    size_t pos = html_str.find("<!-- USERNAME_PLACEHOLDER -->");
    if (pos != string::npos) {
        html_str.replace(pos, 29, username);
    }

    // Pre-fill subject with "Fwd: "
    string fwd_subject = subject.find("Fwd: ") == 0 ? subject : "Fwd: " + subject;
    pos = html_str.find("<input type=\"text\" name=\"subject\" class=\"form-control\" placeholder=\"Enter subject\"");
    if (pos != string::npos) {
        html_str.replace(pos, 84,
            "<input type=\"text\" name=\"subject\" class=\"form-control\" placeholder=\"Enter subject\" value=\"" + fwd_subject + "\"");
    }

    // Pre-fill body with forwarded message
    string fwd_body = "\n\n--- Forwarded Message ---\nFrom: " + from + "\n" + body;
    pos = html_str.find("<textarea name=\"body\" class=\"form-control\" rows=\"10\"");
    if (pos != string::npos) {
        size_t end_pos = html_str.find("</textarea>", pos);
        if (end_pos != string::npos) {
            size_t start_content = html_str.find(">", pos) + 1;
            html_str.replace(start_content, end_pos - start_content, fwd_body);
        }
    }

    return html_str;
}

string handle_delete_email(const string& username, const string& email_id) {
    // Update inbox list with retry loop for CPUT
    bool inbox_updated = false;
    int max_retries = 5;

    for (int attempt = 0; attempt < max_retries && !inbox_updated; attempt++) {
        // Get current inbox list
        string inbox_list;
        if (!g_kv_client->Get(username + "-mailbox", "inbox", inbox_list)) {
            return "<html><head><meta http-equiv='refresh' content='2;url=/inbox'></head><body><div class='alert alert-danger'>Error: Could not access inbox</div></body></html>";
        }

        auto email_ids = split(inbox_list, ',');
        string new_inbox_list;

        // Remove the email_id from the list
        bool found = false;
        for (const auto& id : email_ids) {
            if (id == email_id) {
                found = true;
                continue;
            }
            if (!new_inbox_list.empty()) {
                new_inbox_list += ",";
            }
            new_inbox_list += id;
        }

        if (!found) {
            return "<html><head><meta http-equiv='refresh' content='2;url=/inbox'></head><body><div class='alert alert-warning'>Email not found in inbox</div></body></html>";
        }

        // Try CPUT
        if (g_kv_client->Cput(username + "-mailbox", "inbox", inbox_list, new_inbox_list)) {
            inbox_updated = true;
        }
    }

    if (!inbox_updated) {
        cerr << "[Warning] Failed to update inbox after retries during delete for user: " << username << endl;
        return "<html><head><meta http-equiv='refresh' content='2;url=/inbox'></head><body><div class='alert alert-danger'>Failed to delete email after retries</div></body></html>";
    }

    // Delete the email data
    g_kv_client->Delete(username + "-mailbox", email_id);

    // Return success page with redirect
    return "<html><head><meta http-equiv='refresh' content='2;url=/inbox'></head><body><div class='alert alert-success'>Email deleted successfully! Redirecting...</div></body></html>";
}