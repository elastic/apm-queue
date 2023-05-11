.DEFAULT_GOAL := all
all: test

fmt: tools/go.mod
	@go run -modfile=tools/go.mod github.com/elastic/go-licenser -license=ASL2 .
	@go run -modfile=tools/go.mod golang.org/x/tools/cmd/goimports -local github.com/elastic/ -w .

lint: tools/go.mod
	for dir in $(shell find . -type f -name go.mod -exec dirname '{}' \;); do (cd $$dir && go mod tidy && git diff --stat --exit-code -- go.mod go.sum) || exit $$?; done
	go run -modfile=tools/go.mod honnef.co/go/tools/cmd/staticcheck -checks=all ./...
	go list -m -json $(MODULE_DEPS) | go run -modfile=tools/go.mod go.elastic.co/go-licence-detector \
		-includeIndirect -rules tools/notice/rules.json -validate

.PHONY: clean
clean:
	rm -fr bin

.PHONY: test
test: go.mod
	go test -v ./...

MODULE_DEPS=$(sort $(shell go list -deps -tags=darwin,linux,windows -f "{{with .Module}}{{if not .Main}}{{.Path}}{{end}}{{end}}"))

notice: NOTICE.txt
NOTICE.txt: go.mod tools/go.mod
	go list -m -json $(MODULE_DEPS) | go run -modfile=tools/go.mod go.elastic.co/go-licence-detector \
		-includeIndirect \
		-rules tools/notice/rules.json \
		-noticeTemplate tools/notice/NOTICE.txt.tmpl \
		-noticeOut NOTICE.txt
