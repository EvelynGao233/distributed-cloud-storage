#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <signal.h>
#include <time.h>
#include <netdb.h>
#include <vector>
#include <string>
#include <fstream>
#include <sstream>
#include <iostream>
#include <mutex>

// Frontend server information
struct FrontendServer {
    std::string host;   // e.g. "localhost"
    int port;           // e.g. “8001”
    bool is_alive;
    time_t last_check;

    FrontendServer(const std::string& h, int p)
        : host(h), port(p), is_alive(true), last_check(time(NULL)) {}

    std::string get_address() const {
        return host + ":" + std::to_string(port);
    }
};

// Global state
std::vector<FrontendServer> g_frontend_servers;
std::mutex g_servers_lock;
int g_round_robin_index = 0;
volatile int g_shutting_down = 0;
volatile int g_listen_fd = -1;

// Signal handler for graceful shutdown
void handle_signal(int signal) {
    printf("\n[Load Balancer] Received signal %d, shutting down...\n", signal);
    g_shutting_down = 1;
    if (g_listen_fd >= 0) {
        close(g_listen_fd);
    }
    exit(0);
}

// Parse server address (format: "localhost:8001" or "127.0.0.1:8001")
bool parse_server_address(const std::string& addr, std::string& host, int& port) {
    size_t colon_pos = addr.find(':');
    if (colon_pos == std::string::npos) {
        fprintf(stderr, "[Error] Invalid server address format: %s (expected host:port)\n", addr.c_str());
        return false;
    }

    host = addr.substr(0, colon_pos);
    std::string port_str = addr.substr(colon_pos + 1);

    try {
        port = std::stoi(port_str);
        if (port <= 0 || port > 65535) {
            fprintf(stderr, "[Error] Invalid port number: %d\n", port);
            return false;
        }
        return true;
    } catch (...) {
        fprintf(stderr, "[Error] Cannot parse port: %s\n", port_str.c_str());
        return false;
    }
}

// Load frontend server configuration from file
bool load_frontend_config(const char* config_file) {
    std::ifstream file(config_file);
    if (!file.is_open()) {
        fprintf(stderr, "[Error] Cannot open config file: %s\n", config_file);
        return false;
    }

    std::string line;
    int count = 0;

    while (std::getline(file, line)) {
        if (line.empty() || line[0] == '#') {
            continue;
        }

        line.erase(0, line.find_first_not_of(" \t\r\n"));
        line.erase(line.find_last_not_of(" \t\r\n") + 1);

        if (line.empty()) {
            continue;
        }

        std::string host;
        int port;

        if (parse_server_address(line, host, port)) {
            g_frontend_servers.emplace_back(host, port);
            printf("[Config] Added frontend server: %s\n", line.c_str());
            count++;
        }
    }

    file.close();

    if (count == 0) {
        fprintf(stderr, "[Error] No valid frontend servers found in config\n");
        return false;
    }

    printf("[Config] Loaded %d frontend server(s)\n", count);
    return true;
}

// Check if a frontend server is healthy
bool check_server_health(const std::string& host, int port) {
    int sock_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (sock_fd < 0) {
        return false;
    }

    // Set connection timeout (2 seconds)
    struct timeval timeout;
    timeout.tv_sec = 2;
    timeout.tv_usec = 0;
    setsockopt(sock_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    setsockopt(sock_fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));

    // Resolve hostname
    struct hostent* server = gethostbyname(host.c_str());
    if (server == NULL) {
        close(sock_fd);
        return false;
    }

    // Connect to server
    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    memcpy(&server_addr.sin_addr.s_addr, server->h_addr, server->h_length);
    server_addr.sin_port = htons(port);

    if (connect(sock_fd, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        close(sock_fd);
        return false;
    }

    // Send HTTP GET request to /health
    const char* request = "GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    if (send(sock_fd, request, strlen(request), 0) < 0) {
        close(sock_fd);
        return false;
    }

    // Read response
    char buffer[1024];
    int bytes = recv(sock_fd, buffer, sizeof(buffer) - 1, 0);
    close(sock_fd);

    if (bytes <= 0) {
        return false;
    }

    buffer[bytes] = '\0';

    // Check if response contains "200 OK"
    return (strstr(buffer, "200 OK") != NULL);
}

// Health check thread： periodically checks all frontend servers
void* health_check_thread(void* arg) {
    printf("[Health Check] Thread started\n");
    bool first_check = true;

    while (!g_shutting_down) {
        sleep(5);  // Check health status every 5 seconds

        if (g_shutting_down) break;

        std::lock_guard<std::mutex> lock(g_servers_lock);

        for (auto& server : g_frontend_servers) {
            bool was_alive = server.is_alive;
            server.is_alive = check_server_health(server.host, server.port);
            server.last_check = time(NULL);

            // Log all statuses on first check
            // then only changes on status will be printed
            if (first_check) {
                if (server.is_alive) {
                    printf("[Health Check] Server UP: %s\n", server.get_address().c_str());
                } else {
                    printf("[Health Check] Server DOWN: %s\n", server.get_address().c_str());
                }
            } else {
                // Log status changes
                if (was_alive && !server.is_alive) {
                    printf("[Health Check] Server DOWN: %s\n", server.get_address().c_str());
                } else if (!was_alive && server.is_alive) {
                    printf("[Health Check] Server UP: %s\n", server.get_address().c_str());
                }
            }
        }

        first_check = false;
    }

    printf("[Health Check] Thread stopped\n");
    return NULL;
}

// Select a frontend server using Round Robin strategy
std::string select_frontend_server() {
    std::lock_guard<std::mutex> lock(g_servers_lock);

    if (g_frontend_servers.empty()) {
        return "";
    }

    // Round Robin with health check
    int attempts = 0;
    int num_servers = g_frontend_servers.size();

    while (attempts < num_servers) {
        int idx = (g_round_robin_index++) % num_servers;

        if (g_frontend_servers[idx].is_alive) {
            std::string addr = g_frontend_servers[idx].get_address();
            printf("[Load Balancer] Selected server: %s (Round Robin)\n", addr.c_str());
            return addr;
        }

        attempts++;
    }

    // All servers are down
    fprintf(stderr, "[Load Balancer]  All frontend servers are down!\n");
    return "";
}

// Send HTTP redirect response
void send_redirect(int fd, const std::string& target_server) {
    char response[1024];
    snprintf(response, sizeof(response),
        "HTTP/1.1 302 Found\r\n"
        "Location: http://%s/\r\n"
        "Content-Length: 0\r\n"
        "Connection: close\r\n"
        "\r\n",
        target_server.c_str());

    send(fd, response, strlen(response), 0);
}

// Send 503 Service Unavailable response
void send_503_error(int fd) {
    const char* response =
        "HTTP/1.1 503 Service Unavailable\r\n"
        "Content-Type: text/html\r\n"
        "Content-Length: 157\r\n"
        "Connection: close\r\n"
        "\r\n"
        "<html><head><title>Service Unavailable</title></head>"
        "<body><h1>503 Service Unavailable</h1>"
        "<p>All frontend servers are currently down. Please try again later.</p>"
        "</body></html>";

    send(fd, response, strlen(response), 0);
}

// Handle incoming client connection
void* handle_client(void* arg) {
    int client_fd = *(int*)arg;
    free(arg);

    // Read HTTP request (only need the first line)
    char buffer[4096];
    int bytes = recv(client_fd, buffer, sizeof(buffer) - 1, 0);

    if (bytes <= 0) {
        close(client_fd);
        return NULL;
    }

    buffer[bytes] = '\0';

    // Parse request line
    char method[16], path[256], version[16];
    if (sscanf(buffer, "%s %s %s", method, path, version) != 3) {
        close(client_fd);
        return NULL;
    }

    printf("[Request] %s %s from client\n", method, path);

    // Select a frontend server
    std::string target = select_frontend_server();

    if (target.empty()) {
        // All servers down - send 503 error
        send_503_error(client_fd);
    } else {
        // Redirect to selected frontend server
        send_redirect(client_fd, target);
    }

    close(client_fd);
    return NULL;
}

void print_usage(const char* program_name) {
    printf("Usage: %s [OPTIONS]\n", program_name);
    printf("Options:\n");
    printf("  -p <port>       Load Balancer listening port (default: 8080)\n");
    printf("  -c <config>     Frontend servers config file (default: frontend_servers.txt)\n");
    printf("  -h              Show this help message\n");
}

int main(int argc, char* argv[]) {
    signal(SIGINT, handle_signal);
    signal(SIGTERM, handle_signal);

    int port = 8080;
    const char* config_file = "frontend_servers.txt";

    // Parse command line arguments
    int opt;
    while ((opt = getopt(argc, argv, "p:c:h")) != -1) {
        switch (opt) {
            case 'p':
                port = atoi(optarg);
                break;
            case 'c':
                config_file = optarg;
                break;
            case 'h':
                print_usage(argv[0]);
                return 0;
            default:
                print_usage(argv[0]);
                return 1;
        }
    }

    printf("===========================================\n");
    printf("PennCloud Frontend Load Balancer\n");
    printf("===========================================\n\n");

    // Load frontend server configuration
    if (!load_frontend_config(config_file)) {
        fprintf(stderr, "[Error] Failed to load frontend server config\n");
        return 1;
    }

    printf("\n");

    // Start health check thread
    pthread_t health_thread;
    if (pthread_create(&health_thread, NULL, health_check_thread, NULL) != 0) {
        fprintf(stderr, "[Error] Failed to create health check thread\n");
        return 1;
    }
    pthread_detach(health_thread);

    // Create listening socket
    g_listen_fd = socket(PF_INET, SOCK_STREAM, 0);
    if (g_listen_fd < 0) {
        fprintf(stderr, "[Error] Cannot create socket: %s\n", strerror(errno));
        return 1;
    }

    // Set SO_REUSEADDR to allow quick restart
    int socket_opt = 1;
    if (setsockopt(g_listen_fd, SOL_SOCKET, SO_REUSEADDR, &socket_opt, sizeof(socket_opt)) < 0) {
        fprintf(stderr, "[Error] setsockopt failed: %s\n", strerror(errno));
        close(g_listen_fd);
        return 1;
    }

    // Bind to port
    struct sockaddr_in servaddr;
    memset(&servaddr, 0, sizeof(servaddr));
    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = INADDR_ANY;
    servaddr.sin_port = htons(port);

    if (bind(g_listen_fd, (struct sockaddr*)&servaddr, sizeof(servaddr)) < 0) {
        fprintf(stderr, "[Error] bind() failed on port %d: %s\n", port, strerror(errno));
        close(g_listen_fd);
        return 1;
    }

    // Listen for connections
    if (listen(g_listen_fd, 100) < 0) {
        fprintf(stderr, "[Error] listen() failed: %s\n", strerror(errno));
        close(g_listen_fd);
        return 1;
    }

    printf("[Load Balancer] Ready and listening on port %d\n", port);
    printf("[Load Balancer] Strategy: Round Robin\n");
    printf("[Load Balancer] Health check interval: 5 seconds\n\n");
    printf("Press Ctrl+C to stop.\n\n");

    // Main accept loop
    while (!g_shutting_down) {
        struct sockaddr_in clientaddr;
        socklen_t clientlen = sizeof(clientaddr);

        int client_fd = accept(g_listen_fd, (struct sockaddr*)&clientaddr, &clientlen);

        if (client_fd < 0) {
            if (g_shutting_down) break;

            if (errno == EINTR) continue;

            fprintf(stderr, "[Error] accept() failed: %s\n", strerror(errno));
            continue;
        }

        // Create thread to handle client
        pthread_t thread;
        int* fd_ptr = (int*)malloc(sizeof(int));
        if (!fd_ptr) {
            close(client_fd);
            continue;
        }

        *fd_ptr = client_fd;

        if (pthread_create(&thread, NULL, handle_client, fd_ptr) != 0) {
            fprintf(stderr, "[Error] Failed to create thread: %s\n", strerror(errno));
            free(fd_ptr);
            close(client_fd);
            continue;
        }

        pthread_detach(thread);
    }

    printf("\n[Load Balancer] Shutting down gracefully...\n");
    close(g_listen_fd);

    return 0;
}
