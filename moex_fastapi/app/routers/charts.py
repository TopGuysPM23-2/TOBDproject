from fastapi import APIRouter, Query
from app.clickhouse_utils import fetch_from_clickhouse
from typing import List, Dict

router = APIRouter(prefix="/charts", tags=["charts"])

@router.get("/candles/{ticker}")
async def get_candles_chart(
    ticker: str,
    interval: int = Query(24, description="Таймфрейм свечей: 1,10,60,24"),
    limit: int = Query(100, description="Количество свечей")
) -> List[Dict]:
    """
    Данные для визуализации свечей.
    """
    sql = f"""
    SELECT
        ticker,
        open_price,
        close_price,
        open_dt AS time,
        close_dt,
        landed_at
    FROM td.candles
    WHERE ticker = '{ticker}'
    ORDER BY open_dt DESC
    LIMIT {limit}
    """
    rows = fetch_from_clickhouse(sql)
    return rows