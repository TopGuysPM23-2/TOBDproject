import uvicorn
from fastapi import FastAPI, HTTPException, Query, Depends
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from app.routers import securities, market, history
import httpx
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import asyncio
import logging
from app.dependencies import get_moex_client, MoexISSClient
from concurrent.futures import ThreadPoolExecutor
from confluent_kafka import Producer

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Управление жизненным циклом приложения.
    - При запуске: инициализируем клиенты
    - При остановке: закрываем соединения
    """
    logger.info("🚀 MOEX Proxy API запускается...")
    yield
    logger.info("🛑 MOEX Proxy API останавливается...")


# Настройка Kafka producer
def create_kafka_producer():
    try:
        kafka_broker = 'kafka:9093'
        producer = Producer({
        'bootstrap.servers': kafka_broker,
        'client.id': 'moex-proxy-api-producer',
        'acks': 'all',
        'retries': 5,
        'retry.backoff.ms': 1000,
        'enable.idempotence': True,
        'max.in.flight.requests.per.connection': 1,
        'linger.ms': 5,
        'batch.size': 16384,
        'socket.timeout.ms': 30000,
        'message.timeout.ms': 300000,
        'client.dns.lookup': 'use_all_dns_ips',
        'security.protocol': 'plaintext',
        })
        logger.info(f"Kafka producer created. bootstrap.servers={kafka_broker}, topic=candles")
    except Exception as e:
        logger.error(f"Error creating Kafka producer: {e}")
        raise e

    return producer


producer = create_kafka_producer()


# Создаем приложение с подробным описанием для Swagger
app = FastAPI(
    title="MOEX ISS Proxy API",
    description=""" 
    ## 📡 Прокси-API к Московской бирже (MOEX ISS)
    Этот API является прокси-сервером к официальному API Московской биржи (ISS).
    """,
    version="1.0.0",
    contact={
        "name": "MOEX ISS Proxy API",
        "url": "https://www.moex.com/a2193",
    },
    license_info={
        "name": "MOEX ISS Terms of Use",
        "url": "https://www.moex.com/s116",
    },
    lifespan=lifespan,
    docs_url="/docs",  # Swagger UI
    redoc_url="/redoc",  # ReDoc альтернативная документация
    openapi_tags=[
        {
            "name": "securities",
            "description": "Информация о ценных бумагах (акции, облигации, ETF и т.д.)",
        },
        {
            "name": "market",
            "description": "Текущие рыночные данные (котировки, стаканы, сделки)",
        },
        {
            "name": "history",
            "description": "Исторические данные (свечи, итоги торгов)",
        },
    ],
)

# Настройка CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # В production замените на конкретные домены
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Подключаем роутеры
app.include_router(securities.router)
app.include_router(market.router)
app.include_router(history.router)


# Корневой эндпоинт
@app.get("/", tags=["root"])
async def root():
    """
    Корневой эндпоинт API.
    Возвращает основную информацию о сервисе и доступные эндпоинты.
    """
    return {
        "service": "MOEX ISS Proxy API",
        "description": "Прокси-сервер для доступа к данным Московской биржи",
        "version": "1.0.0",
        "documentation": {
            "swagger": "/docs",
            "redoc": "/redoc",
            "openapi": "/openapi.json"
        },
    }


# Эндпоинт для запроса исторических данных о свечах
@app.get("/candles/{security_id}")
async def get_candles(
        security_id: str,
        interval: int = Query(24, description="Интервал свечей: 1 (1 мин), 10 (10 мин), 60 (1 час), 24 (1 день)"),
        from_date: str = Query(None, description="Начальная дата (YYYY-MM-DD). По умолчанию: 30 дней назад"),
        till_date: str = Query(None, description="Конечная дата (YYYY-MM-DD). По умолчанию: сегодня"),
        limit: int = Query(100, description="Количество свечей", ge=1, le=500),
        client: MoexISSClient = Depends(get_moex_client)
):
    """
    Получение исторических данных о свечах для заданной бумаги.
    """
    logger.info(f"Fetching candles for {security_id} with params - from_date: {from_date}, till_date: {till_date}, "
                f"interval: {interval}, limit: {limit}")

    try:
        # Подготовка параметров
        if not from_date:
            from_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        if not till_date:
            till_date = datetime.now().strftime("%Y-%m-%d")

        # Параметры для запроса
        params = {
            "interval": interval,
            "from": from_date,
            "till": till_date,
            "limit": limit,
        }

        # Логируем параметры запроса
        logger.info(f"Request parameters for {security_id}: {params}")

        # Формирование эндпоинта для запроса
        endpoint = f"engines/stock/markets/shares/boards/TQBR/securities/{security_id}/candles"
        data = await client.fetch_raw(endpoint, params)

        # Логирование полученных данных
        logger.info(f"Fetched data for {security_id}: {data}")

        return data  # Возвращаем полученные данные

    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error while fetching candles for {security_id}: {e.response.text}")
        raise HTTPException(status_code=e.response.status_code, detail=f"Ошибка API: {e.response.text}")
    except Exception as e:
        logger.error(f"Error while fetching candles for {security_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Произошла ошибка: {str(e)}")


# Эндпоинт для обработки и анализа данных с отправкой в Kafka
@app.get("/process_and_analyze")
async def process_and_analyze_data(
        from_date: str = Query(None, description="Начальная дата (YYYY-MM-DD). По умолчанию: 30 дней назад"),
        till_date: str = Query(None, description="Конечная дата (YYYY-MM-DD). По умолчанию: сегодня"),
        limit: int = Query(100, description="Количество свечей", ge=1, le=500),
        interval: int = Query(24, description="Интервал свечей: 1 (1 мин), 10 (10 мин), 60 (1 час), 24 (1 день)"),
):
    try:
        tickers = ["SBER", "GAZP", "LKOH", "YNDX", "NVTK"]
        data = []

        # Логируем входные параметры
        logger.info(f"Processing data with parameters - from_date: {from_date}, till_date: {till_date}, "
                    f"limit: {limit}, interval: {interval}")

        # Если параметры не заданы, используем значения по умолчанию
        if not from_date:
            from_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        if not till_date:
            till_date = datetime.now().strftime("%Y-%m-%d")

        # Логируем обновленные параметры
        logger.info(f"Using dates: from_date: {from_date}, till_date: {till_date}")

        # Вытягиваем данные о свечах для всех тикеров параллельно
        async with httpx.AsyncClient() as client:
            tasks = []
            for ticker in tickers:
                url = f"http://127.0.0.1:8000/candles/{ticker}?interval={interval}&from_date={from_date}&till_date={till_date}&limit={limit}"
                logger.info(f"Request URL for {ticker}: {url}")
                tasks.append(client.get(url))

            responses = await asyncio.gather(*tasks)

            # Обрабатываем ответы
            for response, ticker in zip(responses, tickers):
                if response.status_code == 200:
                    data.append(response.json())
                else:
                    logger.error(f"Failed to fetch data for ticker {ticker}, status code: {response.status_code}")
                    raise HTTPException(status_code=response.status_code, detail="Failed to fetch data")

        # Логируем получение данных
        logger.info(f"Fetched data for tickers: {tickers}")

        # Обработка данных с использованием параллельных вычислений для метрик
        all_processed_data = []

        for ticker, ticker_data in zip(tickers, data):
            candles = ticker_data['candles']['data']
            if candles:
                # Перебираем данные для каждого дня
                for candle in candles:
                    open_price = candle[0]
                    close_price = candle[1]
                    high = candle[2]
                    low = candle[3]
                    value = candle[4]
                    volume = candle[5]
                    begin = candle[6]
                    end = candle[7]

                    # Вычисляем дополнительные метрики
                    sma = (open_price + close_price) / 2  # Пример простого расчета SMA
                    std = high - low  # Пример стандартного отклонения
                    avg_price = (high + low) / 2  # Средняя цена
                    close_to_open_ratio = (close_price - open_price) / open_price  # Отношение цены закрытия к цене открытия

                    # Формируем итоговый объект для каждого дня
                    processed_candle = {
                        "ticker": ticker,
                        "openPrice": open_price,
                        "closePrice": close_price,
                        "openDt": begin,
                        "closeDt": end,
                        "sma": sma,
                        "std": std,
                        "avgPrice": avg_price,
                        "closeToOpenRatio": close_to_open_ratio
                    }

                    all_processed_data.append(processed_candle)

        # Отправка данных в Kafka (оставим часть закомментированной, так как она не изменяется)
        topic = "candles"
        for data in all_processed_data:
            producer.produce(topic, value=str(data))
            logger.info(f"Sent processed data to Kafka topic {topic}")

        # Ждем, пока все сообщения не будут отправлены
        producer.flush()

        # Возвращаем обработанные данные
        logger.info("Processed data successfully and sent to Kafka.")
        return {"processed_data": all_processed_data}

    except Exception as e:
        logger.error(f"Error processing and analyzing data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))



# Функция для расчета метрик
def process_metrics(df, ticker):
    # Расчет простого скользящего среднего (SMA)
    df["SMA"] = df[["open", "close"]].mean(axis=1)  # Среднее между ценой открытия и закрытия за день

    # Стандартное отклонение (STD) — разница между максимальной и минимальной ценой
    df["STD"] = df["high"] - df["low"]

    # Средняя цена за день
    df["avg_price"] = (df["high"] + df["low"]) / 2

    # Отношение между ценой закрытия и ценой открытия (Close to Open Ratio)
    df["close_to_open_ratio"] = (df["close"] - df["open"]) / df["open"]

    # Преобразуем данные обратно в список Python для возврата
    processed_data = df.to_dict(orient='records')

    # Добавляем ticker к данным для удобства
    return {ticker: processed_data}


# Для локального запуска
if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host="127.0.0.1",
        port=8000,
        reload=True,
        log_level="info"
    )
