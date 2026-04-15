# app/core/config.py
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    SERVICE_NAME: str = "booking-service"

    # BD principal del stack (cinema_booking y cinema_catalog viven en el mismo cluster Postgres)
    DATABASE_URL: str

    # JWT — mismo secret que auth-service (fuente de verdad de autenticación)
    JWT_SECRET: str
    JWT_ALGORITHM: str = "HS256"

    # Kafka
    KAFKA_ENABLED: bool = False
    KAFKA_BOOTSTRAP_SERVERS: str = ""
    KAFKA_API_KEY: str = ""
    KAFKA_API_SECRET: str = ""

    # payment-service URL
    PAYMENT_SERVICE_URL: str = "http://payment-service:8003"
    # catalog-service URL — para validar showtimes al crear una compra
    CATALOG_SERVICE_URL: str = "http://catalog-service:8006"
    PAYMENT_RETRY_INTERVAL_SECONDS: int = 30
    PAYMENT_MAX_INIT_ATTEMPTS: int = 3
    PAYMENT_FLOW_STALE_SECONDS: int = 15 * 60
    INVENTORY_DECISION_TIMEOUT_SECONDS: int = 2 * 60

    # Redis — shared blacklist with auth-service (key: blacklist:<sha256>)
    REDIS_URL: str = ""

    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
    }


settings = Settings()
