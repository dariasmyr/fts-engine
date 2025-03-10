# Full-Text Search Test Engine 

This is a simple custom-built full-text search engine in Go with LevelDB key-value storage. It supports loading documents, indexing them, and performing searches on the indexed content using tokenization, stemming, stop-word removal.

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
go build -o build/sso ./cmd/sso
```

Run the service (!Check the configuration file!):

```bash
make execute
# Or manually:
./build/sso --config=./config/config_local.yaml
```

## Test the service:
```bash
make test
# Or manually:
go test -v ./tests -count=1
```

