import uvicorn

if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host="127.0.0.1:8000",  # Доступно со всех интерфейсов
        port=8000,        # Порт по умолчанию
        reload=True,      # Автоматическая перезагрузка при изменениях
        log_level="info", # Уровень логирования
        access_log=True   # Логирование запросов

    )
