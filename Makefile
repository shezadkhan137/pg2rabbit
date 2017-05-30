GO_EXECUTABLE ?= go
DIST_DIRS := find * -type d -exec
VERSION ?= $(shell git describe --tags)

build:
	${GO_EXECUTABLE} build -o pg2rabbit-amd64 main.go

test:
	${GO_EXECUTABLE} test ./pg2rabbit

.PHONY: build test
