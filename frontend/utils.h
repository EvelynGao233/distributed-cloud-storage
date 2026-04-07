#ifndef UTILS_H
#define UTILS_H

#include <string>
#include <vector>
#include <map>

// Authentication & Security
std::string md5_hash(const std::string& str);
std::string generate_token();
std::string generate_email_id();

// URL Encoding/Decoding
std::string url_decode(const std::string& str);
std::string url_encode(const std::string& str);

// Form & Query Parsing
std::map<std::string, std::string> parse_form_data(const std::string& body);
std::map<std::string, std::string> parse_query_params(const std::string& query);
std::string get_query_param(const std::string& url, const std::string& param);
std::string get_post_param(const std::string& body, const std::string& param);

// Cookie Handling
std::string get_cookie_value(const std::string& cookies, const std::string& name);

// String Utilities
std::vector<std::string> split(const std::string& str, char delimiter);
std::string get_current_time();

#endif // UTILS_H