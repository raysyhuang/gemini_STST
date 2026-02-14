"""
Telegram notification module.

Sends a Markdown-formatted daily summary to the configured Telegram chat
after each screener run. Includes market regime, signal count, and
per-ticker details with Finnhub headlines.
"""

import logging
import ssl

import aiohttp
import certifi

from app.config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID

logger = logging.getLogger(__name__)

TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
_ssl_ctx = ssl.create_default_context(cafile=certifi.where())


def _escape_md(text: str) -> str:
    """Escape special Markdown characters for Telegram MarkdownV2."""
    for ch in r"\_*[]()~`>#+-=|{}.!":
        text = text.replace(ch, f"\\{ch}")
    return text


def _build_message(
    screener_result: dict,
    news_map: dict[str, list[dict]],
    reversion_result: dict | None = None,
) -> str:
    """
    Build a unified Telegram MarkdownV2 message with both momentum
    and mean-reversion signals.
    """
    d = screener_result["date"]
    regime = screener_result["regime"]
    signals = screener_result["signals"]
    regime_str = regime.get("regime", "Unknown")

    date_str = _escape_md(str(d))
    regime_display = _escape_md(regime_str)

    # Header
    lines = [
        f"*QuantScreener Daily Report*",
        f"Date: {date_str}",
        f"Market Regime: *{regime_display}*",
        "",
    ]

    if regime_str == "Bearish":
        lines.append(_escape_md("⚠️ Bearish Regime — exercise caution"))
        lines.append("")

    # --- Momentum Section ---
    n_mom = len(signals)
    lines.append(f"*— MOMENTUM BREAKOUTS \\({n_mom}\\) —*")
    lines.append("")

    if not signals:
        lines.append(_escape_md("No momentum signals today."))
        lines.append("")
    else:
        for sig in signals:
            sym = sig["symbol"]
            price = sig["trigger_price"]
            rvol = sig["rvol_at_trigger"]
            atr = sig["atr_pct_at_trigger"]

            sym_esc = _escape_md(sym)
            lines.append(f"*{sym_esc}* — ${_escape_md(str(price))}")
            lines.append(f"  RVOL: {_escape_md(str(rvol))} \\| ATR: {_escape_md(str(atr))}%")

            articles = news_map.get(sym, [])
            for article in articles[:2]:
                headline = _escape_md(article.get("headline", ""))
                lines.append(f"  • {headline}")

            lines.append("")

    # --- Reversion Section ---
    rev_signals = reversion_result.get("signals", []) if reversion_result else []
    n_rev = len(rev_signals)
    lines.append(f"*— OVERSOLD REVERSIONS \\({n_rev}\\) —*")
    lines.append("")

    if not rev_signals:
        lines.append(_escape_md("No oversold reversals today."))
    else:
        for sig in rev_signals:
            sym_esc = _escape_md(sig["symbol"])
            price_esc = _escape_md(str(sig["trigger_price"]))
            rsi_esc = _escape_md(str(sig["rsi2"]))
            dd_esc = _escape_md(str(sig["drawdown_3d_pct"]))
            lines.append(f"*{sym_esc}* — ${price_esc}")
            lines.append(f"  RSI\\(2\\): {rsi_esc} \\| 3d Drop: {dd_esc}%")
            lines.append("")

    return "\n".join(lines)


async def send_telegram_alert(
    screener_result: dict,
    news_map: dict[str, list[dict]] | None = None,
    reversion_result: dict | None = None,
) -> bool:
    """
    Send the unified daily summary (momentum + reversion) to Telegram.

    Returns True on success, False on failure.
    """
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("Telegram credentials not configured — skipping alert")
        return False

    if news_map is None:
        news_map = {}

    message = _build_message(screener_result, news_map, reversion_result)

    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "MarkdownV2",
        "disable_web_page_preview": True,
    }

    try:
        connector = aiohttp.TCPConnector(ssl=_ssl_ctx)
        async with aiohttp.ClientSession(connector=connector) as session:
            async with session.post(TELEGRAM_API, json=payload) as resp:
                if resp.status == 200:
                    logger.info("Telegram alert sent successfully")
                    return True
                else:
                    body = await resp.text()
                    logger.error("Telegram API error %d: %s", resp.status, body)
                    return False
    except Exception as e:
        logger.error("Failed to send Telegram alert: %s", e)
        return False
