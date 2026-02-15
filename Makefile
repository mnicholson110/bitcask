CC ?= cc
CFLAGS ?= -Wall -Wextra -Iinclude
LDFLAGS ?=
LDLIBS ?= 

SRC := $(wildcard src/*.c)
TEST_SRC := test/correctness_test.c
BENCH_SRC := test/benchmark.c

BIN_DIR := bin
TEST_BIN := $(BIN_DIR)/correctness_test
BENCH_BIN := $(BIN_DIR)/benchmark
BENCH_O3_BIN := $(BIN_DIR)/benchmark_O3

.PHONY: clean test bench bench_O3

$(BIN_DIR):
	mkdir -p $(BIN_DIR)

test: $(TEST_BIN)

$(TEST_BIN): $(SRC) $(TEST_SRC) | $(BIN_DIR)
	$(CC) $(CFLAGS) $(SRC) $(TEST_SRC) -o $@ $(LDFLAGS) $(LDLIBS)

bench: $(BENCH_BIN)

$(BENCH_BIN): $(SRC) $(BENCH_SRC) | $(BIN_DIR)
	$(CC) $(CFLAGS) $(SRC) $(BENCH_SRC) -o $@ $(LDFLAGS) $(LDLIBS)

bench_O3: $(BENCH_O3_BIN)

$(BENCH_O3_BIN): $(SRC) $(BENCH_SRC) | $(BIN_DIR)
	$(CC) $(CFLAGS) -O3 $(SRC) $(BENCH_SRC) -o $@ $(LDFLAGS) $(LDLIBS)

clean:
	rm -rf $(BIN_DIR)
