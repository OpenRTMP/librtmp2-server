/**
 * main.c — Test runner for librtmp2-server
 */
#include <stdio.h>

int test_db_main(void);
int test_config_main(void);
int test_http_stats_main(void);

int main(void)
{
    int total_passed = 0;
    int total_tests = 3;

    printf("=== librtmp2-server unit tests ===\n\n");

    printf("--- DB (SQLite) ---\n");
    total_passed += (test_db_main() == 0) ? 1 : 0;

    printf("\n--- Config ---\n");
    total_passed += (test_config_main() == 0) ? 1 : 0;

    printf("\n--- HTTP Stats ---\n");
    total_passed += (test_http_stats_main() == 0) ? 1 : 0;

    printf("\n=== Results: %d/%d suites passed ===\n", total_passed, total_tests);
    return (total_passed == total_tests) ? 0 : 1;
}
