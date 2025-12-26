import os
import clickhouse_connect 
from typing import Optional

CLICKHOUSE_CONFIG = {
    "host": os.getenv("CLICKHOUSE_HOST", "clickhouse"),  # имя сервиса Docker
    "port": int(os.getenv("CLICKHOUSE_PORT", 8123)),      # HTTP порт ClickHouse
    "username": os.getenv("CLICKHOUSE_USER", "default"),
    "password": os.getenv("CLICKHOUSE_PASSWORD", "12345"),
    "database": os.getenv("CLICKHOUSE_DB", "td"),
}

clickhouse_client: Optional[object] = None

def get_clickhouse_client():
    global clickhouse_client
    if clickhouse_client is None:
        clickhouse_client = clickhouse_connect.get_client(
            host=CLICKHOUSE_CONFIG["host"],
            port=CLICKHOUSE_CONFIG["port"],
            username=CLICKHOUSE_CONFIG["username"],
            password=CLICKHOUSE_CONFIG["password"],
            database=CLICKHOUSE_CONFIG["database"]
        )
    return clickhouse_client