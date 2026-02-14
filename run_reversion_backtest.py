"""Full-universe 3-year reversion backtest — run on Heroku one-off dyno."""
import gc, sys, warnings
warnings.filterwarnings("ignore")
import logging
logging.basicConfig(level=logging.ERROR)

from datetime import date, timedelta
from app.database import SessionLocal
from app.models import Ticker
from app.backtester import _load_batch_data, _compute_wide_indicators, _run_batch

to_date = date.today()
from_date = to_date - timedelta(days=365 * 3)

db = SessionLocal()
tickers = db.query(Ticker).filter(Ticker.is_active.is_(True)).all()
id2sym = {t.id: t.symbol for t in tickers}
ids = list(id2sym.keys())
db.close()
print(f"Total tickers: {len(ids)}", flush=True)

results = []
fails = 0
BATCH = 50  # smaller batches to stay within memory

for i in range(0, len(ids), BATCH):
    batch = ids[i : i + BATCH]
    batch_num = i // BATCH + 1
    total_batches = (len(ids) + BATCH - 1) // BATCH
    try:
        db = SessionLocal()
        raw = _load_batch_data(db, batch, from_date, to_date)
        db.close()
        if raw.empty:
            print(f"  batch {batch_num}/{total_batches}: empty", flush=True)
            continue
        p, o, rv, at, rsi2, dd3 = _compute_wide_indicators(
            raw, id2sym, strategy_type="reversion"
        )
        del raw
        gc.collect()
        r = _run_batch(
            p, o, rv, at, strategy_type="reversion", rsi2_df=rsi2, drawdown_3d_df=dd3
        )
        results.extend(r)
        print(
            f"  batch {batch_num}/{total_batches}: +{len(r)} tickers (total {len(results)})",
            flush=True,
        )
        del p, o, rv, at, rsi2, dd3
        gc.collect()
    except Exception as e:
        fails += 1
        print(f"  batch {batch_num}/{total_batches}: FAILED ({e})", flush=True)
        try:
            db.close()
        except:
            pass
        gc.collect()

# Summary
print("\n===== REVERSION BACKTEST RESULTS =====", flush=True)
ht = [r for r in results if r["total_trades"] > 0]
w = [r for r in results if r["total_return_pct"] > 0]
l = [r for r in results if r["total_return_pct"] < 0]
tt = sum(r["total_trades"] for r in results)
ar = sum(r["total_return_pct"] for r in results) / len(results) if results else 0
awr = sum(r["win_rate"] for r in ht) / len(ht) if ht else 0
apf = sum(r["profit_factor"] for r in ht) / len(ht) if ht else 0

print(f"TICKERS={len(results)} FAILED={fails} WITH_TRADES={len(ht)}")
print(f"TOTAL_TRADES={tt} AVG_RET={ar:.2f}pct AVG_WR={awr:.1f}pct AVG_PF={apf:.2f}")
print(f"WINNERS={len(w)} LOSERS={len(l)} FLAT={len(results)-len(w)-len(l)}")

sr = sorted(results, key=lambda x: x["total_return_pct"], reverse=True)
print("TOP10:")
for r in sr[:10]:
    t = r["ticker"]
    ret = r["total_return_pct"]
    tr = r["total_trades"]
    wr = r["win_rate"]
    pf = r["profit_factor"]
    print(f"  {t:6s} ret={ret:+8.1f}pct wr={wr:.1f}pct pf={pf:.2f} trades={tr}")

print("BOT10:")
for r in sr[-10:]:
    t = r["ticker"]
    ret = r["total_return_pct"]
    tr = r["total_trades"]
    wr = r["win_rate"]
    pf = r["profit_factor"]
    print(f"  {t:6s} ret={ret:+8.1f}pct wr={wr:.1f}pct pf={pf:.2f} trades={tr}")

print("BUCKETS:")
for lo, hi, label in [(1, 5, "1-5"), (6, 15, "6-15"), (16, 30, "16-30"), (31, 999, "31+")]:
    b = [r for r in ht if lo <= r["total_trades"] <= hi]
    if b:
        a = sum(r["total_return_pct"] for r in b) / len(b)
        aw = sum(r["win_rate"] for r in b) / len(b)
        tt2 = sum(r["total_trades"] for r in b)
        print(
            f"  {label:5s}: {len(b):4d} tickers {tt2:5d} trades avg_ret={a:+.2f}pct avg_wr={aw:.1f}pct"
        )
