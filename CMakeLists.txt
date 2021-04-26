cmake_minimum_required(VERSION 3.10)
project(TransactionalMemorySimulator)

file(GLOB SOURCES
        src/include/*.h
        src/*.cpp
        )

add_executable(simulator ${SOURCES})

set_property(TARGET simulator PROPERTY CXX_STANDARD 17)