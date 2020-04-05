protoc:
	@echo "Generating Go files"
	cd pkg/proto && protoc -I . server.proto --go_out=plugins=grpc:.