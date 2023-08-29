.PHONY: test

build:
	go build ./...

test:
	go test ./... -list=.

binary:
	go build -o out/sql
