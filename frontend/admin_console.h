#ifndef ADMIN_CONSOLE_H
#define ADMIN_CONSOLE_H

#include <string>

std::string render_admin_page(const std::string &username,
                              const std::string &selected_node_input,
                              const std::string &start_key,
                              const std::string &message_input);

std::string execute_admin_command(const std::string &node,
                                   const std::string &cmd);

std::string handle_admin_command(const std::string &username,
                                 const std::string &node,
                                 const std::string &cmd);

#endif // ADMIN_CONSOLE_H