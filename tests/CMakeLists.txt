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

# Unit test executable
add_executable(run_tests
    unit/main.c
    unit/test_db.c
    unit/test_config.c
    unit/test_http_stats.c
)

target_link_libraries(run_tests PRIVATE lrtmp2server_test_lib)

add_test(NAME unit_tests COMMAND run_tests)
