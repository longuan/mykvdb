
cmake_minimum_required(VERSION 3.16)

project(mykvdb)


set(CMAKE_CXX_STANDARD 17)

set(CMAKE_CXX_FLAGS_DEBUG "$ENV{CXXFLAGS} -Wall -g") 

include_directories(
    include
)

set(TEST_SRC 
    ./src/test.cpp
    ./src/db.cpp
    ./include/leveldb/arena.cpp
)

set(CLI_SRC 
    ./src/client.cpp
    ./src/db.cpp
    ./include/leveldb/arena.cpp
)

add_executable(MyKvTEST ${TEST_SRC})
target_link_libraries(MyKvTEST pthread)

add_executable(MyKvCLI ${CLI_SRC})
target_link_libraries(MyKvCLI pthread)