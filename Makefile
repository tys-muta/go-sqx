.ONESHELL:

define HELP
Usage:
	make [command]

Commands:
	help	ヘルプを表示します
	init	開発用ソフトウェアをインストールします
endef
export HELP

define GO_TOOLS
github.com/spf13/cobra-cli@latest
github.com/golangci/golangci-lint/cmd/golangci-lint@v1.46.2
github.com/cosmtrek/air@v1.40.1
endef
export GO_TOOLS

.PHONY: help
help:
	@echo "$$HELP"

.PHONY: init
init:
	@for v in $$GO_TOOLS; do go install $$v; done

.PHONY: check
check:
	go fmt ./...
	golangci-lint run
	go test -cover ./... -coverprofile=docs/cover/cover.out
	go tool cover -html=docs/cover/cover.out -o docs/cover/cover.html

.PHONY: build
build:
	GOPRIVATE='github.com/10antz-inc' go build -o ./bin/sqx -ldflags '-s -w' ./main.go
