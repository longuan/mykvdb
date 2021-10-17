
cmake_minimum_required(VERSION 3.16)

project(mykvdb)


set(CMAKE_CXX_STANDARD 17)

set(CMAKE_CXX_FLAGS_DEBUG "$ENV{CXXFLAGS} -Wall -g -DDEBUG") 

include_directories(
    include
)

set(SRC_FILES 
    ./src/db.cpp
    ./include/leveldb/arena.cpp
)

add_executable(${PROJECT_NAME} ${SRC_FILES})
