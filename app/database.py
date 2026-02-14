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
    """Create all tables. Call once at startup."""
    import app.models  # noqa: F401 – ensure models are registered
    Base.metadata.create_all(bind=_get_engine())
