.PHONY: dc
dc:
	docker compose up

.PHOMY: producer
producer:
	go run cmd/producer/main.go

.PHOMY: consumer
consumer:
	go run cmd/consumer/main.go