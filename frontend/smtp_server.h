#ifndef SMTP_SERVER_H
#define SMTP_SERVER_H

#include <string>

void start_smtp_server(int port, const std::string& router_address);

void stop_smtp_server();

#endif
