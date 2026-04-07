#include "utils.h"
#include <sstream>
#include <ctime>
#include <cstdio>
#include <cctype>
#include <openssl/md5.h>

using namespace std;

string md5_hash(const string& str) {
    unsigned char digest[MD5_DIGEST_LENGTH];
    MD5((unsigned char*)str.c_str(), str.size(), digest);

    char md5_str[33];
    for (int i = 0; i < MD5_DIGEST_LENGTH; i++) {
        sprintf(&md5_str[i*2], "%02x", (unsigned int)digest[i]);
    }
    md5_str[32] = '\0';
    return string(md5_str);
}

string generate_token() {
    string token = to_string(time(NULL)) + "_" + to_string(rand());
    return md5_hash(token);
}

string generate_email_id() {
    return "email_" + to_string(time(NULL)) + "_" + to_string(rand());
}

string url_encode(const string& str) {
    string result;
    char hex[4];
    
    for (char c : str) {
        if (isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~') {
            result += c;
        } else if (c == ' ') {
            result += '+';
        } else {
            sprintf(hex, "%%%02X", (unsigned char)c);
            result += hex;
        }
    }
    return result;
}

string url_decode(const string& str) {
    string result;
    for (size_t i = 0; i < str.length(); i++) {
        if (str[i] == '%' && i + 2 < str.length()) {
            int value;
            sscanf(str.substr(i + 1, 2).c_str(), "%x", &value);
            result += static_cast<char>(value);
            i += 2;
        } else if (str[i] == '+') {
            result += ' ';
        } else {
            result += str[i];
        }
    }
    return result;
}

map<string, string> parse_form_data(const string& body) {
    map<string, string> form;
    istringstream ss(body);
    string pair;

    while (getline(ss, pair, '&')) {
        size_t eq = pair.find('=');
        if (eq != string::npos) {
            string key = pair.substr(0, eq);
            string value = pair.substr(eq + 1);
            form[key] = url_decode(value);
        }
    }
    return form;
}

map<string, string> parse_query_params(const string& query) {
    return parse_form_data(query);
}

string get_query_param(const string& url, const string& param) {
    size_t query_pos = url.find('?');
    if (query_pos == string::npos) return "";
    
    string query = url.substr(query_pos + 1);
    auto params = parse_query_params(query);
    return params[param];
}

string get_post_param(const string& body, const string& param) {
    auto form = parse_form_data(body);
    return form[param];
}

string get_cookie_value(const string& cookies, const string& name) {
    size_t pos = cookies.find(name + "=");
    if (pos == string::npos) return "";

    pos += name.length() + 1;
    size_t end = cookies.find(';', pos);
    if (end == string::npos) {
        return cookies.substr(pos);
    }
    return cookies.substr(pos, end - pos);
}

string get_current_time() {
    time_t now = time(NULL);
    char buf[64];
    strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", localtime(&now));
    return string(buf);
}

vector<string> split(const string& str, char delimiter) {
    vector<string> tokens;
    string token;
    istringstream ss(str);
    while (getline(ss, token, delimiter)) {
        if (!token.empty()) {
            tokens.push_back(token);
        }
    }
    return tokens;
}