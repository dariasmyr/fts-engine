.PHONY: build run clean tidy execute test test-loader test-index test-search

APP_NAME ?= fts
BUILD_DIR ?= build
OUTPUT := $(BUILD_DIR)/$(APP_NAME)
MAIN_FILE := ./cmd/fts
CONFIG_FILE := ./config/config_local.yaml

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

test-kv:
	go test -v ./internal/services/fts_kv -count=1

test-trie:
	go test -v ./internal/services/fts_trie -count=1
