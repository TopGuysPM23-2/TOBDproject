from fastapi import APIRouter
from typing import List, Dict
from app.clickhouse_utils import fetch_from_clickhouse

router = APIRouter(prefix="/metrics", tags=["metrics"])

@router.get("/ticker/{ticker}")
async def get_metrics(ticker: str, limit: int = 100) -> List[Dict]:
    query = """
        SELECT open_price, close_price, sma, std, avg_price, close_to_open_ratio, open_dt, close_dt
        FROM td.candles
        WHERE ticker = %(ticker)s
        ORDER BY open_dt DESC
        LIMIT %(limit)s
    """
    params = {"ticker": ticker, "limit": limit}
    rows = fetch_from_clickhouse(query, params=params)
    return rows