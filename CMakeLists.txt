cmake_minimum_required(VERSION 3.13)
project(distributed-cloud-storage LANGUAGES CXX)

set(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake")
set(CMAKE_CXX_STANDARD 17)

add_subdirectory(proto)
add_subdirectory(client)
add_subdirectory(server)
add_subdirectory(frontend)
