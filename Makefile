CC ?= gcc
CFLAGS ?= -std=gnu11 -O3 -Wall -Wextra -pthread
LDFLAGS ?= -pthread

SRC_DIR = src
INC_DIR = include
BUILD_DIR = build
TEST_DIR = tests
BENCH_DIR = bench

TARGET = distributed-token-bucket-engine
TEST_TARGET = test_engine
BENCH_TARGET = bench_engine

SRCS := $(wildcard $(SRC_DIR)/*.c)
OBJS := $(patsubst $(SRC_DIR)/%.c, $(BUILD_DIR)/%.o, $(SRCS))

MAIN_OBJ = $(BUILD_DIR)/main.o
LIB_OBJS = $(filter-out $(MAIN_OBJ), $(OBJS))

.PHONY: all build test benchmark clean directories

all: directories build

directories:
	@mkdir -p $(BUILD_DIR) $(TEST_DIR) $(BENCH_DIR) $(INC_DIR) $(SRC_DIR)

build: directories $(BUILD_DIR)/$(TARGET)

$(BUILD_DIR)/$(TARGET): $(OBJS)
	$(CC) $(OBJS) -o $@ $(LDFLAGS)

$(BUILD_DIR)/%.o: $(SRC_DIR)/%.c
	$(CC) $(CFLAGS) -I$(INC_DIR) -c $< -o $@

test: directories
	@if [ -f $(TEST_DIR)/test_main.c ]; then \
		$(CC) $(CFLAGS) -I$(INC_DIR) $(TEST_DIR)/test_main.c $(LIB_OBJS) -o $(BUILD_DIR)/$(TEST_TARGET) $(LDFLAGS); \
		./$(BUILD_DIR)/$(TEST_TARGET); \
	else \
		echo "No test files found in $(TEST_DIR)/. Skipping tests."; \
	fi

benchmark: directories
	@if [ -f $(BENCH_DIR)/bench_main.c ]; then \
		$(CC) $(CFLAGS) -I$(INC_DIR) $(BENCH_DIR)/bench_main.c $(LIB_OBJS) -o $(BUILD_DIR)/$(BENCH_TARGET) $(LDFLAGS); \
		./$(BUILD_DIR)/$(BENCH_TARGET); \
	else \
		echo "No benchmark files found in $(BENCH_DIR)/. Skipping benchmarks."; \
	fi

clean:
	@rm -rf $(BUILD_DIR)