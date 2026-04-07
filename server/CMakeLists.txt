#
# Dependencies
#
find_package(Threads)

#
# Sources
#
set(SOURCES
        src/kv_server.cpp
        src/wal.cpp
        src/checkpoint.cpp
        src/recovery.cpp
)

source_group(TREE ${CMAKE_CURRENT_SOURCE_DIR} FILES ${SOURCES})


#
# Target
#
add_executable(server ${SOURCES})
target_link_libraries(server
        PRIVATE
        myproto
)

#
#router
#
add_executable(router
        src/router_server.cpp
)

target_link_libraries(router
        PRIVATE
        myproto
)

# coordinator
add_executable(coordinator
        src/coordinator_server.cpp
)

target_link_libraries(coordinator
        PRIVATE
        myproto
)
