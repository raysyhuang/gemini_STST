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


def _build_message(screener_result: dict, news_map: dict[str, list[dict]]) -> str:
    """
    Build a Telegram MarkdownV2 message from the screener result.

    Args:
        screener_result: output of run_screener()
        news_map: {symbol: [{"headline":...}, ...]}
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

    if not signals:
        lines.append(_escape_md(f"Screener Run Complete: 0 Signals."))
        return "\n".join(lines)

    lines.append(f"*{len(signals)} Signal{'s' if len(signals) != 1 else ''}:*")
    lines.append("")

    for sig in signals:
        sym = sig["symbol"]
        price = sig["trigger_price"]
        rvol = sig["rvol_at_trigger"]
        atr = sig["atr_pct_at_trigger"]

        sym_esc = _escape_md(sym)
        lines.append(f"*{sym_esc}* — ${_escape_md(str(price))}")
        lines.append(f"  RVOL: {_escape_md(str(rvol))} \\| ATR: {_escape_md(str(atr))}%")

        # Append up to 2 headlines if available
        articles = news_map.get(sym, [])
        for article in articles[:2]:
            headline = _escape_md(article.get("headline", ""))
            lines.append(f"  • {headline}")

        lines.append("")

    return "\n".join(lines)


async def send_telegram_alert(
    screener_result: dict,
    news_map: dict[str, list[dict]] | None = None,
) -> bool:
    """
    Send the daily screener summary to Telegram.

    Returns True on success, False on failure.
    """
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("Telegram credentials not configured — skipping alert")
        return False

    if news_map is None:
        news_map = {}

    message = _build_message(screener_result, news_map)

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
