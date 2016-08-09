cmake_minimum_required(VERSION 2.8)
project(m3bp-doubler)

include_directories(../../include)

set(APP_SOURCES "main.cpp")

add_executable(m3bp-doubler ${APP_SOURCES})
target_link_libraries(m3bp-doubler m3bp)
set_target_properties(m3bp-doubler PROPERTIES COMPILE_FLAGS "-std=c++11 -g -Wall")
