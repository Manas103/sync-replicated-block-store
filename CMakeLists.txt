cmake_minimum_required(VERSION 3.16)
project(sync_replicated_block_store CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

# The machine's /usr/local/bin/protoc (28.x) shadows the apt-installed one
# in PATH, but libprotobuf-dev/libgrpc++-dev here are 3.12.4 / 1.30.2 and
# need codegen from the matching /usr/bin/protoc; mixing them does not
# link. This was a real build break the first time this ran (see README
# Findings), so both tools are pinned to absolute paths rather than PATH
# lookups.
set(PROTOC_BIN /usr/bin/protoc)
set(GRPC_CPP_PLUGIN /usr/bin/grpc_cpp_plugin)

set(PROTO_DIR ${CMAKE_SOURCE_DIR}/proto)
set(GEN_DIR ${CMAKE_BINARY_DIR}/gen)
file(MAKE_DIRECTORY ${GEN_DIR})

function(generate_proto name)
  add_custom_command(
    OUTPUT ${GEN_DIR}/${name}.pb.cc ${GEN_DIR}/${name}.pb.h
           ${GEN_DIR}/${name}.grpc.pb.cc ${GEN_DIR}/${name}.grpc.pb.h
    COMMAND ${PROTOC_BIN}
            --cpp_out=${GEN_DIR} --grpc_out=${GEN_DIR}
            --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN}
            -I${PROTO_DIR} ${PROTO_DIR}/${name}.proto
    DEPENDS ${PROTO_DIR}/${name}.proto
    VERBATIM
  )
endfunction()

generate_proto(replica)
generate_proto(mediator)

add_library(syncrepl_proto STATIC
  ${GEN_DIR}/replica.pb.cc ${GEN_DIR}/replica.grpc.pb.cc
  ${GEN_DIR}/mediator.pb.cc ${GEN_DIR}/mediator.grpc.pb.cc
)
target_include_directories(syncrepl_proto PUBLIC ${GEN_DIR})
target_link_libraries(syncrepl_proto PUBLIC grpc++ grpc gpr protobuf pthread)

add_library(syncrepl_core STATIC
  src/replica_node.cpp
  src/mediator_service.cpp
  src/linearizability_checker.cpp
)
target_include_directories(syncrepl_core PUBLIC src)
target_link_libraries(syncrepl_core PUBLIC syncrepl_proto)

add_executable(replica_node_main src/main_replica.cpp)
target_link_libraries(replica_node_main PRIVATE syncrepl_core)

add_executable(mediator_main src/main_mediator.cpp)
target_link_libraries(mediator_main PRIVATE syncrepl_core)

add_executable(fault_harness tools/fault_harness.cpp)
target_link_libraries(fault_harness PRIVATE syncrepl_core)

option(BUILD_TESTS "Build the GoogleTest suite" ON)
if(BUILD_TESTS)
  include(FetchContent)
  FetchContent_Declare(
    googletest
    URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.tar.gz
  )
  set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
  FetchContent_MakeAvailable(googletest)

  add_executable(syncrepl_tests
    tests/test_linearizability_checker.cpp
    tests/test_replica_sync.cpp
  )
  target_link_libraries(syncrepl_tests PRIVATE syncrepl_core gtest_main pthread)
  target_include_directories(syncrepl_tests PRIVATE src)
endif()
