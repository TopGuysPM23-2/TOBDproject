-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS td.candles
(
    ticker         LowCardinality(String)    COMMENT 'Тикер',
    open_price     Float64                   COMMENT 'Цена открытия',
    close_price    Float64                   COMMENT 'Цена закрытия',
    open_dt        DateTime64(6)             COMMENT 'Дата и время открытия' CODEC (ZSTD(1)),
    close_dt       DateTime64(6)             COMMENT 'Дата и время закрытия' CODEC (ZSTD(1)),
    landed_at      DateTime64(6)             COMMENT 'Дата и время приземления из кафки' CODEC (ZSTD(1))
)
ENGINE = ReplacingMergeTree(landed_at)
ORDER BY (ticker, open_dt, close_dt)
SETTINGS index_granularity = 8192
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS td.candles
-- +goose StatementEnd
