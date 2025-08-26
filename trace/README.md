# github.com/openGemini/observability/storage/jaeger

`storage/jaeger` use openGemini as Jaeger storage backend

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
