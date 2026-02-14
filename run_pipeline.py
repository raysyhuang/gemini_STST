"""One-off script to run the daily pipeline and print results."""
import asyncio
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)

async def main():
    from app.screener import run_daily_pipeline

    result = await run_daily_pipeline()
    regime = result["regime"]["regime"]
    signals = result["signals"]

    print(f"\n{'='*60}")
    print(f"Regime: {regime}")
    print(f"Signals: {len(signals)}")
    print(f"{'='*60}")

    for s in signals:
        sym = s["symbol"]
        price = s["trigger_price"]
        rvol = s["rvol_at_trigger"]
        atr = s["atr_pct_at_trigger"]
        rsi = s.get("rsi_14", "--")
        high52 = s.get("pct_from_52w_high", "--")
        q = s.get("quality_score", 0)
        flow = s.get("options_sentiment", "--")
        conf = " *CONFLUENCE*" if s.get("confluence") else ""
        print(f"  {sym:6s}  ${price:>7.2f}  RVOL={rvol:.2f}  ATR={atr:.1f}%  RSI={rsi}  52w={high52}%  Q={q:.0f}  Flow={flow}{conf}")

    if not signals:
        print("  (no signals)")

asyncio.run(main())
