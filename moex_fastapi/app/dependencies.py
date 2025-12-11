import httpx
from typing import Optional, Dict, Any
import cachetools
import asyncio

class MoexISSClient:
    """
    Клиент для работы с MOEX ISS API.
    
    Особенности:
    - Автоматическое кэширование частых запросов (5 минут)
    - Обработка ошибок ISS
    - Поддержка пагинации
    """
    
    BASE_URL = "https://iss.moex.com/iss"
    
    def __init__(self):
        self.client = httpx.AsyncClient(
            timeout=30.0,
            limits=httpx.Limits(max_connections=10, max_keepalive_connections=5),
            headers={
                "User-Agent": "MOEX-Proxy-API/1.0",
                "Accept": "application/json",
            }
        )
        # Кэш на 5 минут для частых запросов
        self.cache = cachetools.TTLCache(maxsize=100, ttl=300)
    
    async def close(self):
        await self.client.aclose()
    
    async def fetch_raw(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        use_cache: bool = True
    ) -> Dict[str, Any]:
        """
        Получение сырых данных от ISS MOEX.
        
        Args:
            endpoint: Путь к эндпоинту ISS (без .json)
            params: Параметры запроса
            use_cache: Использовать кэширование (по умолчанию True)
            
        Returns:
            Словарь с данными в формате ISS
            
        Raises:
            HTTPException: При ошибках запроса к ISS
        """
        if params is None:
            params = {}
        
        # Ключ для кэша
        cache_key = f"{endpoint}:{str(sorted(params.items()))}"
        
        # Проверка кэша
        if use_cache and cache_key in self.cache:
            return self.cache[cache_key]
        
        url = f"{self.BASE_URL}/{endpoint}.json"
        
        try:
            # Добавляем стандартные параметры если не указаны
            if "limit" not in params:
                params["limit"] = 100
            
            response = await self.client.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            
            # Сохраняем в кэш
            if use_cache:
                self.cache[cache_key] = data
            
            return data
            
        except httpx.HTTPStatusError as e:
            error_detail = f"ISS API error: {e.response.status_code}"
            if e.response.text:
                error_detail += f" - {e.response.text[:200]}"
            raise Exception(error_detail)
        except Exception as e:
            raise Exception(f"Failed to fetch from ISS: {str(e)}")

# Глобальный экземпляр клиента
moex_client = MoexISSClient()

# Dependency для FastAPI
async def get_moex_client() -> MoexISSClient:
    """
    Dependency Injection для получения клиента MOEX.
    
    Возвращает глобальный экземпляр клиента для использования в эндпоинтах.
    """
    return moex_client