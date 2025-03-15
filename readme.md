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
