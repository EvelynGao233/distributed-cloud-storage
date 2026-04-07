#
# KV Client
#

add_library(kv_client_lib
    src/kv_client.cpp
)

target_link_libraries(kv_client_lib
    PUBLIC
    myproto
)

target_include_directories(kv_client_lib
    PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/src
)