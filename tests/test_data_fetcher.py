import asyncio

from app import data_fetcher


class _FakeQuery:
    def __init__(self, db: "_FakeSession", count_value: int):
        self._db = db
        self._count_value = count_value

    def filter(self, *args, **kwargs):
        return self

    def count(self) -> int:
        return self._count_value

    def update(self, values, synchronize_session=False):
        self._db.updated = True
        self._db.updated_values = values
        return 1


class _FakeSession:
    def __init__(self, prev_active_count: int):
        self.prev_active_count = prev_active_count
        self.executed = False
        self.committed = False
        self.updated = False
        self.updated_values = None

    def execute(self, stmt):
        self.executed = True

    def query(self, model):
        return _FakeQuery(self, self.prev_active_count)

    def commit(self):
        self.committed = True


def test_fetch_all_tickers_dedupes_symbols(monkeypatch):
    async def _fake_fetch_range(session, ticker_gte, ticker_lt):
        if ticker_gte == "0":
            return [
                {"symbol": "1234", "exchange": "NASDAQ", "company_name": "Digits Inc"},
                {"symbol": "AAPL", "exchange": "NASDAQ", "company_name": "Apple"},
            ]
        if ticker_gte == "A":
            return [
                {"symbol": "AAPL", "exchange": "NASDAQ", "company_name": "Apple Dup"},
                {"symbol": "", "exchange": "NYSE", "company_name": "Blank"},
                {"exchange": "NYSE", "company_name": "Missing Symbol"},
            ]
        return []

    monkeypatch.setattr(data_fetcher, "_fetch_ticker_range", _fake_fetch_range)

    deduped = asyncio.run(data_fetcher.fetch_all_tickers(session=None))
    symbols = [t["symbol"] for t in deduped]

    assert set(symbols) == {"1234", "AAPL"}
    assert len(symbols) == 2


def test_upsert_tickers_skips_deactivation_on_partial_universe():
    db = _FakeSession(prev_active_count=5000)
    tickers = [
        {"symbol": f"T{i:04d}", "exchange": "NASDAQ", "company_name": f"Name {i}"}
        for i in range(1200)
    ]

    data_fetcher.upsert_tickers(db, tickers)

    assert db.executed is True
    assert db.committed is True
    assert db.updated is False


def test_upsert_tickers_deactivates_when_universe_is_sufficient():
    db = _FakeSession(prev_active_count=5000)
    tickers = [
        {"symbol": f"T{i:04d}", "exchange": "NASDAQ", "company_name": f"Name {i}"}
        for i in range(3500)
    ]

    data_fetcher.upsert_tickers(db, tickers)

    assert db.executed is True
    assert db.committed is True
    assert db.updated is True
    assert db.updated_values == {"is_active": False}
