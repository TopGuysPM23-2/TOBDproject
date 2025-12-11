from fastapi import APIRouter, Depends, HTTPException, Query
from typing import Optional, Dict, Any
from datetime import datetime, timedelta
from app.dependencies import get_moex_client, MoexISSClient

router = APIRouter(prefix="/history", tags=["history"])

@router.get("/candles/{security_id}", summary="Исторические свечи")
async def get_candles(
    security_id: str,
    interval: int = Query(
        24,
        description="Интервал свечей: 1 (1 мин), 10 (10 мин), 60 (1 час), 24 (1 день)",
        enum=[1, 10, 60, 24]
    ),
    from_date: Optional[str] = Query(
        None,
        description="Начальная дата (YYYY-MM-DD). По умолчанию: 30 дней назад"
    ),
    till_date: Optional[str] = Query(
        None,
        description="Конечная дата (YYYY-MM-DD). По умолчанию: сегодня"
    ),
    limit: int = Query(100, description="Количество свечей", ge=1, le=500),
    engine: str = Query("stock", description="Торговая система"),
    market: str = Query("shares", description="Рынок"),
    client: MoexISSClient = Depends(get_moex_client)
) -> Dict[str, Any]:
    """
    Получение исторических свечей (цен OHLCV).
    
    ## Описание
    Ценовые данные за указанный период в формате свечей:
    - Open - цена открытия
    - High - максимальная цена
    - Low - минимальная цена  
    - Close - цена закрытия
    - Volume - объем торгов
    - Value - оборот в деньгах
    
    ## Параметры
    - **security_id**: Код бумаги
    - **interval**: Таймфрейм (1, 10, 60 минут или 24 часа)
    - **from_date**: Дата начала (YYYY-MM-DD)
    - **till_date**: Дата окончания (YYYY-MM-DD)
    - **limit**: Максимальное количество свечей
    - **engine**: Торговая система
    - **market**: Рынок
    
    ## Примеры
    - `GET /history/candles/SBER?interval=24&from=2024-01-01` - дневные свечи с начала года
    - `GET /history/candles/GAZP?interval=60&limit=50` - последние 50 часовых свечей
    - `GET /history/candles/IMOEX?interval=24&from=2023-01-01&till=2023-12-31` - свечи за год
    
    ## Ответ
    Таблица `candles` с колонками:
    - `open`, `high`, `low`, `close` - цены
    - `volume` - объем в штуках
    - `value` - оборот в валюте
    - `begin`, `end` - временной интервал свечи
    """
    try:
        # Устанавливаем дефолтные даты если не указаны
        if not from_date:
            from_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        if not till_date:
            till_date = datetime.now().strftime("%Y-%m-%d")
        
        params = {
            "interval": interval,
            "from": from_date,
            "till": till_date,
            "limit": limit
        }
        
        endpoint = f"engines/{engine}/markets/{market}/securities/{security_id}/candles"
        return await client.fetch_raw(endpoint, params)
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/turnovers", summary="Сводные обороты по рынкам")
async def get_turnovers(
    client: MoexISSClient = Depends(get_moex_client)
) -> Dict[str, Any]:
    """
    Получение сводных оборотов по всем рынкам.
    
    ## Описание
    Текущие значения оборотов торговой сессии по рынкам.
    Полезно для анализа общей активности на бирже.
    
    ## Пример
    `GET /history/turnovers`
    
    ## Ответ
    Две таблицы:
    - `turnovers` - обороты за текущий день
    - `turnoversprevdate` - обороты за предыдущий торговый день
    """
    return await client.fetch_raw("turnovers")