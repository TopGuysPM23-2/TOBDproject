from fastapi import APIRouter, Depends, HTTPException, Query
from typing import Optional, Dict, Any
from app.dependencies import get_moex_client, MoexISSClient

router = APIRouter(prefix="/market", tags=["market"])

@router.get("/{market}/{board}", summary="Список бумаг рынка")
async def get_market_securities(
    market: str,
    board: str,
    engine: str = Query("stock", description="Торговая система"),
    limit: int = Query(50, description="Количество строк", ge=1, le=500),
    start: int = Query(0, description="Начальная строка для пагинации", ge=0),
    client: MoexISSClient = Depends(get_moex_client)
) -> Dict[str, Any]:
    try:
        params = {"limit": limit, "start": start}
        endpoint = f"engines/{engine}/markets/{market}/boards/{board}/securities"
        return await client.fetch_raw(endpoint, params)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{market}/{board}/{security_id}", summary="Котировки бумаги")
async def get_security_quotes(
    market: str,
    board: str,
    security_id: str,
    engine: str = Query("stock", description="Торговая система"),
    client: MoexISSClient = Depends(get_moex_client)
) -> Dict[str, Any]:
    try:
        endpoint = f"engines/{engine}/markets/{market}/boards/{board}/securities/{security_id}"
        return await client.fetch_raw(endpoint)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{market}/{board}/{security_id}/orderbook", summary="Стакан заявок")
async def get_orderbook(
    market: str,
    board: str,
    security_id: str,
    engine: str = Query("stock", description="Торговая система"),
    depth: int = Query(10, description="Глубина стакана", ge=1, le=50),
    client: MoexISSClient = Depends(get_moex_client)
) -> Dict[str, Any]:
    try:
        params = {"depth": depth}
        endpoint = f"engines/{engine}/markets/{market}/boards/{board}/securities/{security_id}/orderbook"
        return await client.fetch_raw(endpoint, params)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{market}/{board}/{security_id}/trades", summary="Сделки по бумаге")
async def get_trades(
    market: str,
    board: str,
    security_id: str,
    engine: str = Query("stock", description="Торговая система"),
    limit: int = Query(20, description="Количество последних сделок", ge=1, le=100),
    client: MoexISSClient = Depends(get_moex_client)
) -> Dict[str, Any]:
    try:
        params = {"limit": limit}
        endpoint = f"engines/{engine}/markets/{market}/boards/{board}/securities/{security_id}/trades"
        return await client.fetch_raw(endpoint, params)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Справочники - здесь Query используется правильно (все параметры - query)
@router.get("/engines", summary="Список торговых систем")
async def get_engines(
    client: MoexISSClient = Depends(get_moex_client)
) -> Dict[str, Any]:
    return await client.fetch_raw("engines")

@router.get("/engines/{engine}/markets", summary="Рынки торговой системы")
async def get_engine_markets(
    engine: str,
    client: MoexISSClient = Depends(get_moex_client)
) -> Dict[str, Any]:
    return await client.fetch_raw(f"engines/{engine}/markets")

@router.get("/engines/{engine}/markets/{market}/boards", summary="Режимы торгов рынка")
async def get_market_boards(
    engine: str,
    market: str,
    client: MoexISSClient = Depends(get_moex_client)
) -> Dict[str, Any]:
    return await client.fetch_raw(f"engines/{engine}/markets/{market}/boards")