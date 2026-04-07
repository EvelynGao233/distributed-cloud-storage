// frontend_node_registration.h
#ifndef FRONTEND_NODE_REGISTRATION_H
#define FRONTEND_NODE_REGISTRATION_H

#include <string>

void start_frontend_registration(const std::string &my_address,
                                 const std::string &coordinator_addr);

void stop_frontend_registration();

void start_frontend_grpc_control_server(const std::string &listen_addr);

void stop_frontend_grpc_control_server();

bool should_frontend_accept_connections();

#endif // FRONTEND_NODE_REGISTRATION_H