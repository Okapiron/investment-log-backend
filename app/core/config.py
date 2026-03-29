from pydantic import AliasChoices, Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "Trade Trace API"
    app_version: str = Field(
        default="dev-local",
        validation_alias=AliasChoices("APP_VERSION", "VERSION"),
    )
    api_prefix: str = "/api/v1"
    database_url: str = Field(
        default="sqlite:///./app.db",
        validation_alias=AliasChoices("DATABASE_URL", "APP_DATABASE_URL"),
    )
    auth_enabled: bool = Field(
        default=False,
        validation_alias=AliasChoices("AUTH_ENABLED", "APP_AUTH_ENABLED"),
    )
    private_mode_enabled: bool = Field(
        default=False,
        validation_alias=AliasChoices("PRIVATE_MODE_ENABLED", "APP_PRIVATE_MODE_ENABLED"),
    )
    private_mode_secret: str = Field(
        default="",
        validation_alias=AliasChoices("PRIVATE_MODE_SECRET", "APP_PRIVATE_MODE_SECRET"),
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
    openai_api_key: str = Field(
        default="",
        validation_alias=AliasChoices("OPENAI_API_KEY", "APP_OPENAI_API_KEY"),
    )
    openai_model: str = Field(
        default="gpt-4.1-mini",
        validation_alias=AliasChoices("OPENAI_MODEL", "APP_OPENAI_MODEL"),
    )
    openai_base_url: str = Field(
        default="https://api.openai.com/v1/responses",
        validation_alias=AliasChoices("OPENAI_BASE_URL", "APP_OPENAI_BASE_URL"),
    )
    openai_timeout_ms: int = Field(
        default=12000,
        validation_alias=AliasChoices("OPENAI_TIMEOUT_MS", "APP_OPENAI_TIMEOUT_MS"),
    )
    analysis_cache_ttl_seconds: int = Field(
        default=300,
        validation_alias=AliasChoices("ANALYSIS_CACHE_TTL_SECONDS", "APP_ANALYSIS_CACHE_TTL_SECONDS"),
    )
    analysis_mock_enabled: bool = Field(
        default=False,
        validation_alias=AliasChoices("ANALYSIS_MOCK_ENABLED", "APP_ANALYSIS_MOCK_ENABLED"),
    )
    price_provider: str = Field(
        default="yahoo_unofficial",
        validation_alias=AliasChoices("PRICE_PROVIDER", "APP_PRICE_PROVIDER"),
    )
    price_cache_ttl_seconds: int = Field(
        default=43200,
        validation_alias=AliasChoices("PRICE_CACHE_TTL_SECONDS", "APP_PRICE_CACHE_TTL_SECONDS"),
    )
    price_history_days: int = Field(
        default=400,
        validation_alias=AliasChoices("PRICE_HISTORY_DAYS", "APP_PRICE_HISTORY_DAYS"),
    )
    marketstack_access_key: str = Field(
        default="",
        validation_alias=AliasChoices("MARKETSTACK_ACCESS_KEY", "APP_MARKETSTACK_ACCESS_KEY"),
    )
    marketstack_base_url: str = Field(
        default="https://api.marketstack.com/v2",
        validation_alias=AliasChoices("MARKETSTACK_BASE_URL", "APP_MARKETSTACK_BASE_URL"),
    )
    marketstack_jp_mic: str = Field(
        default="XTKS",
        validation_alias=AliasChoices("MARKETSTACK_JP_MIC", "APP_MARKETSTACK_JP_MIC"),
    )
    yahoo_chart_base_url: str = Field(
        default="https://query1.finance.yahoo.com/v8/finance/chart",
        validation_alias=AliasChoices("YAHOO_CHART_BASE_URL", "APP_YAHOO_CHART_BASE_URL"),
    )
    yahoo_user_agent: str = Field(
        default=(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/123.0.0.0 Safari/537.36"
        ),
        validation_alias=AliasChoices("YAHOO_USER_AGENT", "APP_YAHOO_USER_AGENT"),
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
