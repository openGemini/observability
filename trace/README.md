# github.com/openGemini/observability/storage/jaeger

`trace` use openGemini as trace(Jaeger) storage backend

## Design

TODO

## Requirements

- Go 1.24+

## Develop

Install the `buf` tool:

```shell
go install github.com/bufbuild/buf/cmd/buf@latest
```

Generate proto file:

```shell
cd trace && buf generate
```

## Install & Usage

```shell
go install github.com/openGemini/observability/trace/cmd/ts-trace@latest

./ts-trace --config=config.yaml
```
Please refer to the configuration file: [config.example.yaml](./config.example.yaml)
