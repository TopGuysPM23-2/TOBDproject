from app.db import get_clickhouse_client
from typing import Any, List, Dict

def fetch_from_clickhouse(query: str, params: Dict[str, Any] = None) -> List[Dict]:
    client = get_clickhouse_client()
    result = client.query(query, parameters=params)
    return [dict(row) for row in result.result_rows]