# Tests for librtmp2-server

# Test library (reuses server sources, excludes cli.c)
add_library(lrtmp2server_test_lib STATIC
    ../src/config.c
    ../src/db.c
    ../src/logger.c
)

target_include_directories(lrtmp2server_test_lib
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(lrtmp2server_test_lib
    PUBLIC
        pthread
        m
        SQLite::SQLite3
)

# Unit test executable — main.c calls each test_*_main() function.
# New tests are added by: (1) implementing test_<name>_main() in a .c file,
# (2) #including its declaration in main.c, (3) adding it here as a source.
add_executable(run_tests
    unit/main.c
    unit/test_db.c
    unit/test_config.c
    unit/test_http_stats.c
    unit/test_stream_id.c
    unit/test_keygen.c
)

target_link_libraries(run_tests PRIVATE lrtmp2server_test_lib)

add_test(NAME unit_tests COMMAND run_tests)

# ---------------------------------------------------------------------------
# Interop tests — end-to-end tests that spawn the real server binary
# and drive it via librtmp2 client API + HTTP/curl.
# ---------------------------------------------------------------------------
find_package(CURL)
if(NOT CURL_FOUND)
    message(STATUS "CURL not found — interop tests will be skipped")
endif()

if(CURL_FOUND)

add_executable(run_interop_tests
    integration/main.c
    integration/test_interop_obs.c
    integration/test_interop_ffmpeg.c
    integration/test_interop_haishinkkit.c
    integration/test_interop_concurrent.c
)

target_include_directories(run_interop_tests
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
        ${LRTMP2_DIR}/include
)

target_link_libraries(run_interop_tests
    PRIVATE
        ${LRTMP2_A}
        lrtmp2server_test_lib
        CURL::libcurl
        pthread
        m
)

if(ENABLE_TLS)
    # Pre-built librtmp2.a carries unresolved OpenSSL symbols when built with TLS.
    target_link_libraries(run_interop_tests PRIVATE OpenSSL::SSL OpenSSL::Crypto)
endif()

# Interop tests need the server binary to be built first
add_dependencies(run_interop_tests librtmp2-server)

# Run via CTest with a longer timeout — these are end-to-end tests
add_test(NAME interop_tests COMMAND run_interop_tests)
set_tests_properties(interop_tests PROPERTIES
    TIMEOUT 60
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}  # librtmp2-server binary lives here, not in tests/
    DEPENDS unit_tests)  # run after unit tests pass

endif()  # CURL_FOUND
