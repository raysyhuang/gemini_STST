from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from app.config import DATABASE_URL

Base = declarative_base()

# Lazy engine creation — avoids crash when DATABASE_URL is not yet configured
_engine = None
_SessionLocal = None


def _get_engine():
    global _engine
    if _engine is None:
        if not DATABASE_URL:
            raise RuntimeError(
                "DATABASE_URL is not set. Add it to your .env file "
                "(Heroku Postgres connection string)."
            )
        _engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    return _engine


def SessionLocal():
    """Return a new database session."""
    engine = _get_engine()
    factory = sessionmaker(bind=engine, autocommit=False, autoflush=False)
    return factory()


def get_db():
    """FastAPI dependency that yields a database session."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db():
    """Create all tables and run safe column migrations. Call once at startup."""
    import app.models  # noqa: F401 – ensure models are registered
    engine = _get_engine()
    Base.metadata.create_all(bind=engine)

    # Safe column migrations — ADD COLUMN IF NOT EXISTS is idempotent
    _migrate_new_columns(engine)


def _migrate_new_columns(engine):
    """Add columns introduced after initial table creation."""
    from sqlalchemy import text

    migrations = [
        # Phase 5 Sprint 2: Options flow columns
        "ALTER TABLE screener_signals ADD COLUMN IF NOT EXISTS options_sentiment VARCHAR(10)",
        "ALTER TABLE screener_signals ADD COLUMN IF NOT EXISTS put_call_ratio FLOAT",
        "ALTER TABLE reversion_signals ADD COLUMN IF NOT EXISTS options_sentiment VARCHAR(10)",
        "ALTER TABLE reversion_signals ADD COLUMN IF NOT EXISTS put_call_ratio FLOAT",
        # Phase 6: Quality scoring & confluence columns
        "ALTER TABLE screener_signals ADD COLUMN IF NOT EXISTS quality_score FLOAT",
        "ALTER TABLE screener_signals ADD COLUMN IF NOT EXISTS confluence BOOLEAN DEFAULT FALSE",
        "ALTER TABLE reversion_signals ADD COLUMN IF NOT EXISTS quality_score FLOAT",
        "ALTER TABLE reversion_signals ADD COLUMN IF NOT EXISTS confluence BOOLEAN DEFAULT FALSE",
        # Phase 6: Paper trade quality score
        "ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS quality_score FLOAT",
    ]
    with engine.connect() as conn:
        for sql in migrations:
            conn.execute(text(sql))
        conn.commit()
