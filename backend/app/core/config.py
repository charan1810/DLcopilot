from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

# Resolve env files from project root (three levels up: core -> app -> backend -> root)
_PROJECT_ROOT = Path(__file__).resolve().parents[3]
_ENV_FILES = (
    _PROJECT_ROOT / ".env",
    _PROJECT_ROOT / ".env.local",
)


class Settings(BaseSettings):
    APP_NAME: str = "Data Lifecycle Copilot"
    APP_ENV: str = "dev"
    APP_HOST: str = "0.0.0.0"
    APP_PORT: int = 8000

    APP_DB_URL: str
    SECRET_KEY: str

    JWT_SECRET: str = ""  # falls back to SECRET_KEY if empty
    JWT_ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 480

    model_config = SettingsConfigDict(
        env_file=tuple(str(path) for path in _ENV_FILES),
        extra="ignore",
    )


settings = Settings()