include .env
export $(shell sed 's/=.*//' .env)

CH_DSN=http://localhost:8123?username=${CLICKHOUSE_USER}&password=${CLICKHOUSE_PASSWORD}

KAFKA_CONTAINER_NAME=tobdproject-kafka-1

# Команды для работы с докером
dc-up:
	docker-compose up -d

dc-down:
	docker-compose down -v

# Команды для работы с clickhouse
ch-status:
	goose -dir ./migrations clickhouse "$(CH_DSN)" status

ch-up:
	goose -dir ./migrations clickhouse "$(CH_DSN)" up

ch-down:
	goose -dir ./migrations clickhouse "$(CH_DSN)" down

ch-create:
	goose -dir ./migrations create _ sql

# Команды для работы с кафкой
kafka-tlist:
	docker exec "$(KAFKA_CONTAINER_NAME)" kafka-topics --list --bootstrap-server localhost:9092

kafka-tcreate:
	docker exec "$(KAFKA_CONTAINER_NAME)" kafka-topics \
	--create \
	--topic candles \
	--bootstrap-server localhost:9092 \
	--partitions 1 \
	--replication-factor 1 \
	--if-not-exists

kafka-cgcreate:
	docker-compose exec kafka kafka-consumer-groups \
	--bootstrap-server localhost:9092 \
	--group clickhouse_consumer \
	--reset-offsets \
	--to-earliest \
	--all-topics \
	--execute

kafka-cglist:
	docker-compose exec kafka kafka-consumer-groups \
	--bootstrap-server localhost:9092 \
	--list

kafka-test:
	echo "{\"ticker\":\"ticker1\",\"openPrice\":\"1.19\",\"closePrice\":\"2.19\",\"openDt\":\"2025-10-10 10:10:10\",\"closeDt\":\"2025-10-11 10:10:10\"}" | \
	docker exec -i tobdproject-kafka-1 kafka-console-producer \
		--topic candles \
		--bootstrap-server localhost:9092