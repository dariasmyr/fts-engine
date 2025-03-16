# Full-Text Search Test Engine 

A custom-built full-text search engine in Go, with inverted indexed based on prefix trees (tries) and n-grams, with concurrent processing document. The engine handles tokenization, stemming, stop-word removal, and now even supports fuzzy search (partial words and typos). 

![Demo](docs/demo.gif)

## Configuration
Configuration files are located in `./config/`. Use `config_prod_example.yaml` as a template. 

## Install dependencies:

```bash
make tidy
# Or manually:
go mod tidy
```

## Download the wiki dump

The Wikipedia abstract dump can be downloaded from Dump example: https://dumps.wikimedia.your.org/enwiki/latest/enwiki-latest-abstract1.xml.gz. Extract the file and place it in the `./data/` directory.

```bash

## Build and Run
Build the service:

```bash
make build
# Or manually:
go build -o build/fts ./cmd/fts
```

Run the service (!Check the configuration file!):

```bash
make execute
# Or manually:
./build/fts --config=./config/config_local.yaml
```

## Test

```bash
#Test the trie implementation
make test-trie
# Or manually:
go test -v ./internal/services/fts_trie -count=1

#Test the key-value implementation
make test-kv
# Or manually:
go test -v ./internal/services/fts_kv -count=1
```