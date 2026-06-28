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
