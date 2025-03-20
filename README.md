# consulsync

Sync local path to consul

## Install

Install consulsync

```bash
go install github.com/WqyJh/consulsync/cmd/consulsync@latest
```

Install consulfetch

```bash
go install github.com/WqyJh/consulsync/cmd/consulfetch@latest
```

## Usage

Sync local path to consul

```bash
consulsync -local-path path/to/local \
-consul-path path/to/consul \
-consul-addr http://127.0.0.1:8500 \
-consul-token your_token
```

Fetch consul path to local path

```bash
consulfetch -local-path path/to/local \
-consul-path path/to/consul \
-consul-addr http://127.0.0.1:8500 \
-consul-token your_token
```
