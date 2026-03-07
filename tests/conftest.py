from collections.abc import Generator

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from app.db.base import Base
from app.core.config import settings
from app.db.session import get_db
from app.main import app


@event.listens_for(Engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()


@pytest.fixture()
def client() -> Generator[TestClient, None, None]:
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
        future=True,
    )
    TestingSessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
    Base.metadata.create_all(bind=engine)

    def override_get_db() -> Generator[Session, None, None]:
        db = TestingSessionLocal()
        try:
            yield db
        finally:
            db.close()

    app.dependency_overrides[get_db] = override_get_db
    app.state.testing_session_local = TestingSessionLocal
    with TestClient(app) as c:
        yield c
    if hasattr(app.state, "testing_session_local"):
        delattr(app.state, "testing_session_local")
    app.dependency_overrides.clear()


@pytest.fixture(autouse=True)
def disable_auth_by_default() -> Generator[None, None, None]:
    prev_enabled = settings.auth_enabled
    prev_secret = settings.supabase_jwt_secret
    prev_invite_required = settings.invite_code_required
    prev_supabase_url = settings.supabase_url
    prev_supabase_service_role_key = settings.supabase_service_role_key
    prev_ops_alert_target = settings.ops_alert_target
    prev_db_backup_strategy = settings.db_backup_strategy
    prev_cors_allow_origins = settings.cors_allow_origins
    prev_rate_limit_enabled = settings.rate_limit_enabled
    prev_app_version = settings.app_version
    settings.auth_enabled = False
    settings.supabase_jwt_secret = ""
    settings.invite_code_required = False
    settings.supabase_url = ""
    settings.supabase_service_role_key = ""
    settings.ops_alert_target = ""
    settings.db_backup_strategy = ""
    settings.cors_allow_origins = "*"
    settings.rate_limit_enabled = False
    settings.app_version = "dev-local"
    try:
        yield
    finally:
        settings.auth_enabled = prev_enabled
        settings.supabase_jwt_secret = prev_secret
        settings.invite_code_required = prev_invite_required
        settings.supabase_url = prev_supabase_url
        settings.supabase_service_role_key = prev_supabase_service_role_key
        settings.ops_alert_target = prev_ops_alert_target
        settings.db_backup_strategy = prev_db_backup_strategy
        settings.cors_allow_origins = prev_cors_allow_origins
        settings.rate_limit_enabled = prev_rate_limit_enabled
        settings.app_version = prev_app_version
