cmake_minimum_required(VERSION 3.20)
project(glide-cpp-benchmark)

find_package(Threads REQUIRED)
find_package(absl REQUIRED)
find_package(Protobuf REQUIRED)

# Common compilation settings
set(CMAKE_BUILD_TYPE Debug)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(ENV{LD_LIBRARY_PATH} "${CMAKE_SOURCE_DIR}/../../cpp/target/release:$ENV{LD_LIBRARY_PATH}")
set(CMAKE_BUILD_RPATH "${CMAKE_SOURCE_DIR}/../../cpp/target/release")


# Debug flags
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -O0")

if(APPLE)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -stdlib=libc++")
endif()

# RPATH settings
set(CMAKE_INSTALL_RPATH "${CMAKE_CURRENT_BINARY_DIR}")
set(CMAKE_BUILD_WITH_INSTALL_RPATH TRUE)
set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

include(../../cpp/build/glide-cpp-targets.cmake)


# Benchmark executable
add_executable(${PROJECT_NAME} valkey_benchmark.cc)

target_include_directories(${PROJECT_NAME} PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/../../cpp/include
)

target_link_directories(${PROJECT_NAME} PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/../../cpp/target/release/
)

target_link_libraries(${PROJECT_NAME}
    PRIVATE
    glide-cpp
    Threads::Threads
    ${CMAKE_DL_LIBS}
    absl::log_internal_check_op
    glide_rs
    protobuf::libprotobuf
)

# Protobuf specific settings for executable
target_compile_definitions(${PROJECT_NAME} PRIVATE
    GOOGLE_PROTOBUF_USE_DLLS
)

# Set position independent code for executable as well
set_target_properties(${PROJECT_NAME} PROPERTIES
    POSITION_INDEPENDENT_CODE ON
)
