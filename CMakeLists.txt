project (pftool)
cmake_minimum_required (VERSION 3.0)

# enable the unit tests
#enable_testing()

# has the release been set?
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

# setup compiler
#set (C_FLAGS_ALL "-Wall -Wextra -Wno-unused-parameter")
set (C_FLAGS_DEBUG "-Og")

set (CMAKE_C_COMPILER "mpicc")
#set (CMAKE_C_STANDARD "99")
set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${C_FLAGS_ALL} -std=c99")
set (CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} ${C_FLAGS_DEBUG}")
set (CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} ${C_FLAGS_RELEASE}")

set (CMAKE_CXX_COMPILER "mpic++")
#set (CMAKE_CXX_STANDARD "98")
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${C_FLAGS_ALL} -std=c++98")
set (CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} ${C_FLAGS_DEBUG}")
set (CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} ${C_FLAGS_RELEASE}")

# set output directories
SET(EXECUTABLE_OUTPUT_PATH ${mfagent_BINARY_DIR}/bin)
SET(TEST_OUTPUT_PATH ${mfagent_BINARY_DIR}/test)
SET(LIBRARY_OUTPUT_PATH ${mfagent_BINARY_DIR}/lib)

include_directories(${CMAKE_CURRENT_SOURCE_DIR})

add_subdirectory (src)
add_subdirectory (scripts)

