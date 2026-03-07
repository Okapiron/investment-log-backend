from pydantic import AliasChoices, Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "Trade Trace API"
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
    supabase_url: str = Field(
        default="",
        validation_alias=AliasChoices("SUPABASE_URL", "APP_SUPABASE_URL"),
    )
    supabase_service_role_key: str = Field(
        default="",
        validation_alias=AliasChoices("SUPABASE_SERVICE_ROLE_KEY", "APP_SUPABASE_SERVICE_ROLE_KEY"),
    )
    invite_code_required: bool = Field(
        default=False,
        validation_alias=AliasChoices("INVITE_CODE_REQUIRED", "APP_INVITE_CODE_REQUIRED"),
    )
    cors_allow_origins: str = Field(
        default="*",
        validation_alias=AliasChoices("CORS_ALLOW_ORIGINS", "APP_CORS_ALLOW_ORIGINS"),
    )
    rate_limit_enabled: bool = Field(
        default=False,
        validation_alias=AliasChoices("RATE_LIMIT_ENABLED", "APP_RATE_LIMIT_ENABLED"),
    )
    rate_limit_per_minute: int = Field(
        default=120,
        validation_alias=AliasChoices("RATE_LIMIT_PER_MINUTE", "APP_RATE_LIMIT_PER_MINUTE"),
    )
    ops_alert_target: str = Field(
        default="",
        validation_alias=AliasChoices("OPS_ALERT_TARGET", "APP_OPS_ALERT_TARGET"),
    )
    db_backup_strategy: str = Field(
        default="",
        validation_alias=AliasChoices("DB_BACKUP_STRATEGY", "APP_DB_BACKUP_STRATEGY"),
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
