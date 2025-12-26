-- +goose Up
-- +goose StatementBegin
CREATE MATERIALIZED VIEW IF NOT EXISTS td.candles_queue_mv TO td.candles AS
SELECT
    JSONExtractString(msg, 'ticker')                                       as ticker,
    toFloat64(JSONExtractString(msg, 'openPrice'))                         as open_price,
    toFloat64(JSONExtractString(msg, 'closePrice'))                        as close_price,
    toFloat64(JSONExtractString(msg, 'sma'))                               as sma,
    toFloat64(JSONExtractString(msg, 'std'))                               as std,
    toFloat64(JSONExtractString(msg, 'avgPrice'))                          as avg_price,
    toFloat64(JSONExtractString(msg, 'closeToOpenRatio'))                  as close_to_open_ratio,
    parseDateTimeBestEffortOrZero(JSONExtractString(msg, 'openDt'), 6)     as open_dt,
    parseDateTimeBestEffortOrZero(JSONExtractString(msg, 'closeDt'), 6)    as close_dt,
    now64(6)                                                               as landed_at
FROM td.candles_queue;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP VIEW IF EXISTS td.candles_queue_mv;
-- +goose StatementEnd