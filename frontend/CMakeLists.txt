#
# Dependencies
#
find_package(Threads REQUIRED)
find_package(OpenSSL REQUIRED)

#
# Frontend Sources
#
set(FRONTEND_SOURCES
        http_server.cc
        accounts_webmail.cc
        smtp_client.cc
        utils.cc
        storage.cc
        admin_console.cc
        frontend_node_registration.cc
)

#
# Frontend Target
#
add_executable(frontend ${FRONTEND_SOURCES})

target_link_libraries(frontend
        PRIVATE
        myproto
        kv_client_lib
        OpenSSL::SSL
        OpenSSL::Crypto
        Threads::Threads
        resolv
)

target_include_directories(frontend
        PRIVATE
        ${CMAKE_SOURCE_DIR}/proto
        ${CMAKE_BINARY_DIR}/proto
)

#
# Load Balancer Target
#
add_executable(load_balancer load_balancer.cc)

target_link_libraries(load_balancer
        PRIVATE
        Threads::Threads
)

#
# SMTP Server Target
#
set(SMTP_SOURCES
        smtp_server.cc
        utils.cc
)

add_executable(smtp_server ${SMTP_SOURCES})

target_link_libraries(smtp_server
        PRIVATE
        myproto
        kv_client_lib
        OpenSSL::SSL
        OpenSSL::Crypto
        Threads::Threads
)

target_include_directories(smtp_server
        PRIVATE
        ${CMAKE_SOURCE_DIR}/proto
        ${CMAKE_BINARY_DIR}/proto
)