"""Full-universe 3-year reversion backtest — memory-efficient version."""
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

# Running aggregates instead of storing all results
total_tickers = 0
total_trades = 0
total_with_trades = 0
winners = 0
losers = 0
sum_return = 0.0
sum_wr = 0.0
sum_pf = 0.0
fails = 0
# Keep only top/bottom 10 by return
top10 = []  # (return, ticker, trades, wr, pf)
bot10 = []
# Bucket counters
buckets = {
    "1-5": {"count": 0, "trades": 0, "sum_ret": 0.0, "sum_wr": 0.0},
    "6-15": {"count": 0, "trades": 0, "sum_ret": 0.0, "sum_wr": 0.0},
    "16-30": {"count": 0, "trades": 0, "sum_ret": 0.0, "sum_wr": 0.0},
    "31+": {"count": 0, "trades": 0, "sum_ret": 0.0, "sum_wr": 0.0},
}

BATCH = 50

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
        batch_results = _run_batch(
            p, o, rv, at, strategy_type="reversion", rsi2_df=rsi2, drawdown_3d_df=dd3
        )
        del p, o, rv, at, rsi2, dd3
        gc.collect()

        # Aggregate without keeping full results
        for r in batch_results:
            total_tickers += 1
            total_trades += r["total_trades"]
            sum_return += r["total_return_pct"]
            if r["total_trades"] > 0:
                total_with_trades += 1
                sum_wr += r["win_rate"]
                sum_pf += r["profit_factor"]
                # Bucket
                t_count = r["total_trades"]
                if 1 <= t_count <= 5:
                    bk = "1-5"
                elif 6 <= t_count <= 15:
                    bk = "6-15"
                elif 16 <= t_count <= 30:
                    bk = "16-30"
                else:
                    bk = "31+"
                buckets[bk]["count"] += 1
                buckets[bk]["trades"] += t_count
                buckets[bk]["sum_ret"] += r["total_return_pct"]
                buckets[bk]["sum_wr"] += r["win_rate"]
            if r["total_return_pct"] > 0:
                winners += 1
            elif r["total_return_pct"] < 0:
                losers += 1
            # Track top/bottom 10
            entry = (r["total_return_pct"], r["ticker"], r["total_trades"], r["win_rate"], r["profit_factor"])
            top10.append(entry)
            top10.sort(key=lambda x: x[0], reverse=True)
            top10 = top10[:10]
            bot10.append(entry)
            bot10.sort(key=lambda x: x[0])
            bot10 = bot10[:10]

        print(
            f"  batch {batch_num}/{total_batches}: +{len(batch_results)} (total {total_tickers})",
            flush=True,
        )
        del batch_results
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
ar = sum_return / total_tickers if total_tickers else 0
awr = sum_wr / total_with_trades if total_with_trades else 0
apf = sum_pf / total_with_trades if total_with_trades else 0

print(f"TICKERS={total_tickers} FAILED={fails} WITH_TRADES={total_with_trades}")
print(f"TOTAL_TRADES={total_trades} AVG_RET={ar:.2f}pct AVG_WR={awr:.1f}pct AVG_PF={apf:.2f}")
print(f"WINNERS={winners} LOSERS={losers} FLAT={total_tickers - winners - losers}")

print("TOP10:")
for ret, t, tr, wr, pf in top10:
    print(f"  {t:6s} ret={ret:+8.1f}pct wr={wr:.1f}pct pf={pf:.2f} trades={tr}")

print("BOT10:")
for ret, t, tr, wr, pf in bot10:
    print(f"  {t:6s} ret={ret:+8.1f}pct wr={wr:.1f}pct pf={pf:.2f} trades={tr}")

print("BUCKETS:")
for label in ["1-5", "6-15", "16-30", "31+"]:
    b = buckets[label]
    if b["count"] > 0:
        a = b["sum_ret"] / b["count"]
        aw = b["sum_wr"] / b["count"]
        print(f"  {label:5s}: {b['count']:4d} tickers {b['trades']:5d} trades avg_ret={a:+.2f}pct avg_wr={aw:.1f}pct")
