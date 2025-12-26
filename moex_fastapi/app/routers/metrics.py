from fastapi import APIRouter
from app.db import get_clickhouse_client
from typing import List, Dict

router = APIRouter(prefix="/metrics")

@router.get("/ticker/{ticker}")
async def get_metrics(ticker: str, limit: int = 100):
    client = get_clickhouse_client()
    query = """
        SELECT open_price, close_price, sma, std, avg_price, close_to_open_ratio, open_dt, close_dt
        FROM td.candles FINAL
        WHERE ticker = %(ticker)s
        ORDER BY open_dt DESC
        LIMIT %(limit)s
    """
    result = client.query(query, parameters={"ticker": ticker, "limit": limit})
    return [dict(row) for row in result.result_rows]