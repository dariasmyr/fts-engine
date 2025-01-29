.PHONY: build run clean tidy execute test

APP_NAME ?= fts
BUILD_DIR ?= build
OUTPUT := $(BUILD_DIR)/$(APP_NAME)
MAIN_FILE := ./cmd/fts
CONFIG_FILE := ./config/config_local.yaml

STORAGE_PATH := ./storage/fts-test.db

TEST_PKG := fts/tests

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
	go test -v $(TEST_PKG) -count=1