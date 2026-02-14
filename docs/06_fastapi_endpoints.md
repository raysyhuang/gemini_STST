# FastAPI & Frontend Integration Plan

### 1. Application Structure
We will use `main.py` as the application entry point, but we must strictly separate our Pydantic schemas, database dependencies, and API routes.
* **Static Files:** FastAPI must mount the `static/` directory using `StaticFiles`. Navigating to the root URL (`/`) should automatically serve our `index.html` frontend dashboard.
* **CORS Configuration:** Enable CORS via `fastapi.middleware.cors.CORSMiddleware` to allow the Javascript frontend to fetch data seamlessly.

### 2. Pydantic Schemas (Data Validation)
Use these models to ensure the JSON sent to the frontend is strictly typed and formatted perfectly for the UI.

```python
from pydantic import BaseModel
from typing import List, Dict, Any
from datetime import date

class SignalResponse(BaseModel):
    ticker: str
    company_name: str
    date: date
    trigger_price: float
    rvol_at_trigger: float
    atr_pct_at_trigger: float

    class Config:
        orm_mode = True # Allows Pydantic to read directly from SQLAlchemy models

class BacktestResultResponse(BaseModel):
    ticker: str
    win_rate: float
    profit_factor: float
    total_return_pct: float
    max_drawdown_pct: float
    # Formatted specifically for TradingView Lightweight Charts
    # Format: [{"time": "YYYY-MM-DD", "value": 10000.00}]
    equity_curve: List[Dict[str, Any]]