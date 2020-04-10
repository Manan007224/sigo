USE_VENDOR ?= no

ifeq ($(USE_VENDOR),no)
else
  MOD_FLAG := -mod=vendor
endif

protoc:
	@echo "Generating Go files"
	cd pkg/proto && protoc -I . server.proto --go_out=plugins=grpc:.

test:
	@go test -v ${MOD_FLAG} \
	`go list ./pkg/...` 2>&1