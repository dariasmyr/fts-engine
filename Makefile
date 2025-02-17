.PHONY: build run clean tidy execute test test-loader test-index test-search

APP_NAME ?= fts
BUILD_DIR ?= build
OUTPUT := $(BUILD_DIR)/$(APP_NAME)
MAIN_FILE := ./cmd/fts
CONFIG_FILE := ./config/config_local.yaml

TEST_DIR := ./tests

build:
	mkdir -p $(BUILD_DIR)
	go build -ldflags="-s -w" -o $(OUTPUT) $(MAIN_FILE)

run: build
	$(OUTPUT) --config=$(CONFIG_FILE)

clean:
	rm -rf $(BUILD_DIR)

tidy:
	go mod tidy

execute: build
	./$(OUTPUT) --config=$(CONFIG_FILE)

test:
	go test -v $(TEST_DIR) -count=1

test-loader:
	go test -v $(TEST_DIR)/loader_test.go -count=1

test-index:
	go test -v $(TEST_DIR)/index_test.go -count=1

test-search:
	go test -v $(TEST_DIR)/search_test.go -count=1
