from pydantic import AliasChoices, Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "Asset Management MVP"
    api_prefix: str = "/api/v1"
    database_url: str = Field(
        default="sqlite:///./app.db",
        validation_alias=AliasChoices("DATABASE_URL", "APP_DATABASE_URL"),
    )
    auth_enabled: bool = Field(
        default=False,
        validation_alias=AliasChoices("AUTH_ENABLED", "APP_AUTH_ENABLED"),
    )
    supabase_jwt_secret: str = Field(
        default="",
        validation_alias=AliasChoices("SUPABASE_JWT_SECRET", "APP_SUPABASE_JWT_SECRET"),
    )

    @model_validator(mode="after")
    def normalize_database_url(self):
        url = str(self.database_url or "").strip()
        if url.startswith("postgres://"):
            self.database_url = "postgresql+psycopg://" + url[len("postgres://") :]
        elif url.startswith("postgresql://"):
            self.database_url = "postgresql+psycopg://" + url[len("postgresql://") :]
        return self

    model_config = SettingsConfigDict(env_file=".env", env_prefix="APP_")


settings = Settings()
