import os
from dotenv import load_dotenv

load_dotenv()

# Polygon.io (Paid Tier - no rate limits)
POLYGON_API_KEY: str = os.getenv("POLYGON_API_KEY", "").strip()

# Finnhub (News / Catalysts)
FINNHUB_API_KEY: str = os.getenv("FINNHUB_API_KEY", "").strip()

# Heroku Postgres - CRITICAL: no SQLite allowed
DATABASE_URL: str = os.getenv("DATABASE_URL", "").strip()

# Heroku sometimes provides postgres:// but SQLAlchemy 2.x requires postgresql://
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

# Async driver URL for asyncpg
ASYNC_DATABASE_URL: str = DATABASE_URL.replace(
    "postgresql://", "postgresql+asyncpg://", 1
) if DATABASE_URL else ""
