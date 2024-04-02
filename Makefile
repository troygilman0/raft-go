docker:
	docker-compose down --remove-orphans
	docker-compose up --build --scale node=5

dev:
	@go run .

test:
	@go clean -testcache
	@go test -v ./raft
