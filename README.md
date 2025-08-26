# observability

![License](https://img.shields.io/badge/license-Apache2.0-green)
![Language](https://img.shields.io/badge/Language-Go-blue.svg)
[![Go report](https://goreportcard.com/badge/github.com/opengemini/observability)](https://goreportcard.com/report/github.com/opengemini/observability)
[![Godoc](http://img.shields.io/badge/docs-go.dev-blue.svg?style=flat-square)](https://pkg.go.dev/github.com/openGemini/observability)

`observability` is used to convert observable data (trace, metrics, logs) into a universal OpenGemini architecture.

## Requirements

- Go 1.24+

## Narrative

| plugin name                        | function effect                          |
|------------------------------------|------------------------------------------|
| [storage/jaeger](storage/jaeger) | use openGemini as Jaeger storage backend |

## About OpenGemini

OpenGemini is a cloud-native distributed time series database, find more information [here](https://github.com/openGemini/openGemini)
