#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#define SSD_SIZE 4
#define PACKAGE_SIZE 10
#define DIE_SIZE 2
#define PLANE_SIZE 8
#define BLOCK_SIZE 200
#define OVERPROVISIONING 0.05
#include "746FlashSim.h"

static FILE *log_file_stream;
static char log_file_path[255];

int main(int argc, char *argv[]) {
    if (argc != 3) {
        printf("usage: test_2_6 <config_file_name> <log_file_path>\n");
        exit(EXIT_FAILURE);
    }
    int ret = 1;
    strcpy(log_file_path, argv[2]);
    log_file_stream = fopen(log_file_path, "w+");
    assert(log_file_stream != NULL);

    fprintf(log_file_stream, "------------------------------------------------------------\n");

    init_flashsim();

    int r;
    srand(15746);
    const size_t num_raw_blocks = SSD_SIZE * PACKAGE_SIZE * DIE_SIZE * PLANE_SIZE;
    const size_t num_nondata_blocks = OVERPROVISIONING * num_raw_blocks;

    assert(num_nondata_blocks < BLOCK_SIZE);

    uint64_t total_writes = 0;
    size_t num_live_page = 2 + (rand() % (BLOCK_SIZE - 3));
    size_t num_block_touched = 0;
    FlashSimTest test(argv[1]);
    while (test.TotalErasesPerformed() == 0) {
        if (num_block_touched <= num_nondata_blocks) {
            if (num_block_touched == 0) {
                num_live_page = BLOCK_SIZE;
            } else if (num_block_touched == 1) {
                num_live_page = BLOCK_SIZE - 1;
            }
            const size_t block_start = num_block_touched * BLOCK_SIZE;
            for (size_t addr = block_start; addr < block_start + num_live_page; addr++) {
                r = test.Write(log_file_stream, addr, rand() % 18746);
                if (r != 1) goto failed;
                if (test.TotalErasesPerformed() != 0) {
                    fprintf(log_file_stream, "Doing GC unnecessarily\n");
                    goto failed;
                }
            }
            total_writes = test.TotalWritesPerformed();
            r = test.Write(log_file_stream, block_start, 18746);
            if (r != 1) goto failed;
            TEST_PAGE_TYPE page_value;
            r = test.Read(log_file_stream, block_start, &page_value);
            if (r != 1) goto failed;
            if (page_value != 18746) {
                fprintf(log_file_stream, "Reading LBA %zu does not get the right value\n", block_start);
                goto failed;
            }
            if (test.TotalErasesPerformed() == 0) {
                if (num_block_touched != 0) {
                    // Touch block-0 again to make sure it is never least-recent-used
                    r = test.Write(log_file_stream, 0, 18746);
                    if (r != 1) goto failed;
                }
            }
            num_block_touched++;
            num_live_page = 2 + (rand() % (BLOCK_SIZE - 3));
        } else {
            fprintf(log_file_stream, "No GC activity detected\n");
            goto failed;
        }
    }

    fprintf(log_file_stream, ">>> GC detected <<<\n");

    if (test.TotalErasesPerformed() != 3) {
        fprintf(log_file_stream, "Too less or too many erase operations\n");
        goto failed;
    } else {
        if (num_block_touched <= 1) {
            fprintf(log_file_stream, "Doing GC unnecessarily\n");
            goto failed;
        } else {
            if (test.TotalWritesPerformed() - total_writes - 1 != 2 * BLOCK_SIZE) {
                fprintf(log_file_stream, "We think a wrong block has been cleaned\n");
                goto failed;
            }
        }
    }

    srand(15746);
    num_live_page = 2 + (rand() % (BLOCK_SIZE - 3));
    for (size_t i = 0; i < num_block_touched; i++) {
        const size_t block_start = i * BLOCK_SIZE;
        if (i == 0) {
            num_live_page = BLOCK_SIZE;
        } else if (i == 1) {
            num_live_page = BLOCK_SIZE - 1;
        }
        size_t addr = block_start;
        for (; addr < block_start + num_live_page; addr++) {
            TEST_PAGE_TYPE expected = rand() % 18746;
            if (addr == block_start) {
                expected = 18746;
            }
            TEST_PAGE_TYPE page_value;
            r = test.Read(log_file_stream, addr, &page_value);
            if (r != 1) goto failed;
            if (page_value != expected) {
                fprintf(log_file_stream, "Reading LBA %zu does not get the right value\n", addr);
                goto failed;
            }
        }
        for (; addr < BLOCK_SIZE; addr++) {
            TEST_PAGE_TYPE ignored;
            r = test.Read(log_file_stream, addr, &ignored);
            if (r != 0) {
                fprintf(log_file_stream, "Reading LBA %zu should not return anything\n", addr);
                goto failed;
            }
        }
        num_live_page = 2 + (rand() % (BLOCK_SIZE - 3));
    }

    ret = 0;
    printf("SUCCESS ...Check %s for more details.\n", log_file_path);
    goto done;
failed:
    printf("FAILED ...Check %s for more details.\n", log_file_path);
done:
    fflush(log_file_stream);
    fclose(log_file_stream);

    deinit_flashsim();

    return ret;
}
