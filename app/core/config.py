from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "Asset Management MVP"
    api_prefix: str = "/api/v1"
    database_url: str = "sqlite:///./app.db"

    model_config = SettingsConfigDict(env_file=".env", env_prefix="APP_")


settings = Settings()
