docker:
	docker-compose down --remove-orphans
	docker-compose up --build --scale node=5

dev:
	go run .