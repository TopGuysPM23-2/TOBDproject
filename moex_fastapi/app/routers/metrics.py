# app/routers/metrics_router.py
from fastapi import APIRouter
from typing import List, Dict
import pandas as pd
from app.db import get_clickhouse_client
from app.metrics import process_metrics

router = APIRouter(prefix="/metrics", tags=["metrics"])

@router.post("/update")
async def update_metrics(candles: List[Dict]):
    if not candles:
        return {"status": "no data"}

    df = pd.DataFrame(candles)
    ticker = df["ticker"].iloc[0]  # берём первый тикер (можно расширить на несколько)
    
    # Вычисляем метрики
    df = process_metrics(df, ticker)
    
    # Подключаемся к ClickHouse
    client = get_clickhouse_client()

    # Вставляем данные в таблицу
    rows = df.to_dict(orient="records")
    columns = ", ".join(rows[0].keys())
    values_template = ", ".join([f"%({col})s" for col in rows[0].keys()])
    insert_query = f"INSERT INTO td.candles ({columns}) VALUES ({values_template})"

    client.command(insert_query, params=rows)
    return {"status": "ok", "inserted_rows": len(rows)}