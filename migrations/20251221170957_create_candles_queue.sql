-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS td.candles_queue
(
    `msg` String
) 
ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'kafka:9092',           
    kafka_topic_list = 'candles',               
    kafka_group_name = 'clickhouse_consumer',  
    kafka_num_consumers = 1,
    kafka_format = 'JSONAsString'
;         
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS td.candles_queue;
-- +goose StatementEnd