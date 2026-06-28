cmake_minimum_required(VERSION 3.16)
project(librtmp2-server VERSION 0.1.0 LANGUAGES C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

# Options
option(ENABLE_TESTS "Build tests" ON)
# RTMPS / TLS via OpenSSL. On by default to match librtmp2's default build;
# turn OFF (-DENABLE_TLS=OFF) for a plaintext-only, OpenSSL-free build.
option(ENABLE_TLS "RTMPS/TLS support via OpenSSL" ON)

if(ENABLE_TLS)
    find_package(OpenSSL REQUIRED)
endif()

# --- librtmp2 (from workspace sibling) ---
set(LRTMP2_DIR "${CMAKE_CURRENT_SOURCE_DIR}/librtmp2" CACHE PATH "librtmp2 source dir")

if(NOT EXISTS "${LRTMP2_DIR}/include/librtmp2/librtmp2.h")
    message(FATAL_ERROR "librtmp2 not found at ${LRTMP2_DIR}. "
                        "Clone it as a sibling: git clone https://github.com/OpenRTMP/librtmp2.git")
endif()

# Use the pre-built .a if available, otherwise build from sources
set(LRTMP2_A "${LRTMP2_DIR}/liblibrtmp2.a")
if(NOT EXISTS "${LRTMP2_A}")
    message(STATUS "Building librtmp2 from sources...")
    file(GLOB_RECURSE LRTMP2_SRCS "${LRTMP2_DIR}/src/*.c")
    add_library(librtmp2_static STATIC ${LRTMP2_SRCS})
    target_include_directories(librtmp2_static PUBLIC "${LRTMP2_DIR}/include" "${LRTMP2_DIR}/src")
    if(ENABLE_TLS)
        # Match librtmp2's own default build so the from-source path also has TLS.
        target_compile_definitions(librtmp2_static PRIVATE LRTMP2_HAVE_TLS)
        target_link_libraries(librtmp2_static PUBLIC OpenSSL::SSL OpenSSL::Crypto)
    endif()
    set(LRTMP2_A librtmp2_static)
endif()

# --- Mongoose (embedded HTTP) ---
include(FetchContent)
FetchContent_Declare(
    mongoose
    GIT_REPOSITORY https://github.com/cesanta/mongoose.git
    GIT_TAG 7.14
)
FetchContent_MakeAvailable(mongoose)

# --- SQLite (system library) ---
find_package(SQLite3 REQUIRED)

# --- Server library ---
# mongoose.c (the amalgamated HTTP engine) must be compiled into the library;
# FetchContent only makes its sources available, it does not build a linkable
# target here, so http.c's mongoose symbols would otherwise be undefined at link.
add_library(lrtmp2server_lib STATIC
    src/server.c
    src/config.c
    src/db.c
    src/http.c
    src/rtmp_callbacks.c
    src/logger.c
    ${mongoose_SOURCE_DIR}/mongoose.c
)

target_include_directories(lrtmp2server_lib
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
    PRIVATE
        ${LRTMP2_DIR}/include
        ${LRTMP2_DIR}/src
        ${mongoose_SOURCE_DIR}
)

target_link_libraries(lrtmp2server_lib
    PUBLIC
        pthread
        m
        SQLite::SQLite3
    PRIVATE
        ${LRTMP2_A}
)

if(ENABLE_TLS)
    # Link OpenSSL into the consumer too: when librtmp2 is a pre-built archive
    # (the LRTMP2_A path above) it carries unresolved OpenSSL symbols that the
    # final link must satisfy.
    target_compile_definitions(lrtmp2server_lib PRIVATE LRTMP2_HAVE_TLS)
    target_link_libraries(lrtmp2server_lib PUBLIC OpenSSL::SSL OpenSSL::Crypto)
endif()

# Main executable
add_executable(librtmp2-server src/cli.c)
target_link_libraries(librtmp2-server PRIVATE lrtmp2server_lib)

# Tests
if(ENABLE_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()

# Install
install(TARGETS librtmp2-server DESTINATION bin)
install(FILES config.example.json DESTINATION etc RENAME config.json)
