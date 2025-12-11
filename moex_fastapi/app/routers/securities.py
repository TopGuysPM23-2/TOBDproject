from fastapi import APIRouter, Depends, HTTPException, Query
from typing import Optional, Dict, Any
from app.dependencies import get_moex_client, MoexISSClient

router = APIRouter(prefix="/securities", tags=["securities"])

@router.get("/{security_id}", summary="Получить информацию о бумаге")
async def get_security_info(
    security_id: str,
    client: MoexISSClient = Depends(get_moex_client)
) -> Dict[str, Any]:
    """
    Получение полной информации о ценной бумаге.
    
    ## Описание
    Возвращает спецификацию инструмента, включая:
    - Основные параметры (ISIN, название, тип)
    - Доступные режимы торгов
    - Эмитент и другие детали
    
    ## Параметры
    - **security_id**: Код ценной бумаги (например: SBER, GAZP, IMOEX)
    
    ## Примеры
    - `GET /securities/SBER` - информация об акциях Сбербанка
    - `GET /securities/IMOEX` - информация об индексе МосБиржи
    
    ## Ответ
    Возвращает сырой ответ от ISS MOEX в формате:
    ```json
    {
      "description": {
        "metadata": {...},
        "data": [...]
      },
      "boards": {
        "metadata": {...},
        "data": [...]
      }
    }
    ```
    """
    try:
        return await client.fetch_raw(f"securities/{security_id}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{security_id}/indices", summary="Получить индексы бумаги")
async def get_security_indices(
    security_id: str,
    client: MoexISSClient = Depends(get_moex_client)
) -> Dict[str, Any]:
    """
    Получение списка индексов, в которые входит бумага.
    
    ## Описание
    Показывает в какие индексы Московской биржи включена указанная бумага.
    
    ## Параметры
    - **security_id**: Код ценной бумаги
    
    ## Пример
    `GET /securities/SBER/indices` - индексы, содержащие Сбербанк
    
    ## Ответ
    Таблица `indices` с колонками:
    - `secid` - Код индекса
    - `shortname` - Краткое название
    - `from` - Дата включения в индекс
    - `till` - Дата исключения из индекса
    """
    try:
        return await client.fetch_raw(f"securities/{security_id}/indices")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{security_id}/aggregates", summary="Агрегированные итоги торгов")
async def get_security_aggregates(
    security_id: str,
    date: Optional[str] = Query(None, description="Дата в формате YYYY-MM-DD"),
    client: MoexISSClient = Depends(get_moex_client)
) -> Dict[str, Any]:
    """
    Получение агрегированных итогов торгов за дату.
    
    ## Описание
    Суммарные показатели торгов по бумаге за указанную дату 
    по всем рынкам и режимам торгов.
    
    ## Параметры
    - **security_id**: Код ценной бумаги
    - **date**: Дата в формате YYYY-MM-DD (опционально, по умолчанию последняя доступная)
    
    ## Пример
    `GET /securities/SBER/aggregates?date=2024-01-15`
    
    ## Ответ
    Агрегированные данные по рынкам:
    - Объемы торгов
    - Количество сделок
    - Средневзвешенная цена
    """
    try:
        params = {}
        if date:
            params["date"] = date
        return await client.fetch_raw(f"securities/{security_id}/aggregates", params)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("", summary="Поиск бумаг")
async def search_securities(
    q: str = Query(..., description="Поисковый запрос"),
    limit: int = Query(20, description="Количество результатов", ge=1, le=100),
    client: MoexISSClient = Depends(get_moex_client)
) -> Dict[str, Any]:
    """
    Поиск ценных бумаг по названию или коду.
    
    ## Описание
    Поиск бумаг торгуемых на Московской бирже.
    Поддерживает поиск по коду, ISIN, названию эмитента.
    
    ## Параметры
    - **q**: Поисковый запрос (минимум 2 символа)
    - **limit**: Максимальное количество результатов (1-100)
    
    ## Примеры
    - `GET /securities?q=сбер` - поиск бумаг содержащих "сбер"
    - `GET /securities?q=RU0009029540` - поиск по ISIN
    - `GET /securities?q=газпром&limit=10`
    
    ## Ответ
    Таблица `securities` с результатами поиска.
    """
    try:
        if len(q) < 2:
            raise HTTPException(status_code=400, detail="Search query must be at least 2 characters")
        
        params = {"q": q, "limit": limit}
        return await client.fetch_raw("securities", params)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))