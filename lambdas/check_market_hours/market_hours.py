"""
Shared utility — determines which exchanges were recently open.

Checks whether each exchange was open at any point within the last
LOOKBACK_MINUTES (default 90 min). This ensures the pipeline runs at
least once after a market closes, capturing the final close price.

Each exchange entry in exchanges.json looks like:
{
  "NASDAQ": {
    "timezone": "America/New_York",
    "open_time": "09:30",
    "close_time": "16:00",
    "holidays": ["2026-01-01", "2026-07-04", ...]
  },
  ...
}
"""

from __future__ import annotations

from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

from config_loader import load_exchanges

LOOKBACK_MINUTES = 90  # check if market was open within last 1h30m


def get_open_exchanges(now_utc: datetime | None = None) -> list[str]:
    """
    Return exchange codes that were open at any point in the last
    LOOKBACK_MINUTES. This catches markets that just closed so we
    always collect the final close price.
    """
    if now_utc is None:
        now_utc = datetime.now(tz=ZoneInfo("UTC"))

    exchanges_cfg = load_exchanges()
    open_exchanges: list[str] = []

    for code, cfg in exchanges_cfg.items():
        tz = ZoneInfo(cfg["timezone"])
        local_now = now_utc.astimezone(tz)
        local_date_str = local_now.strftime("%Y-%m-%d")

        # Skip public holidays
        if local_date_str in cfg.get("holidays", []):
            continue

        # Skip weekends
        if local_now.weekday() >= 5:
            continue

        open_h, open_m = map(int, cfg["open_time"].split(":"))
        close_h, close_m = map(int, cfg["close_time"].split(":"))
        market_open = time(open_h, open_m)
        market_close = time(close_h, close_m)

        # Check if market is open NOW or was open within the lookback window
        current_time = local_now.time().replace(second=0, microsecond=0)
        lookback_time = (
            (local_now - timedelta(minutes=LOOKBACK_MINUTES))
            .time()
            .replace(second=0, microsecond=0)
        )

        # Market is open now
        if market_open <= current_time < market_close:
            open_exchanges.append(code)
        # Market closed within the lookback window (closed after lookback_time)
        elif lookback_time < market_close <= current_time:
            open_exchanges.append(code)

    return open_exchanges


# Wait this long after the close so the closing auction is done and Yahoo's
# daily bar is final; we then collect each exchange's day exactly once.
SETTLE_AFTER_CLOSE = timedelta(hours=2)


def get_exchanges_ready_for_daily(
    now_utc: datetime, last_collected: dict[str, str]
) -> list[dict]:
    """
    Exchanges whose trading day is finished and not yet collected.

    Ready when, in the exchange's own timezone (so DST is automatic):
    today is a trading day (not weekend/holiday), it is at least
    SETTLE_AFTER_CLOSE past today's close, and last_collected[exchange] is
    before today. start_date is the day after the last collected day, so a
    missed day is fetched by the next run; finished days are never refetched.
    """
    ready: list[dict] = []
    for code, cfg in load_exchanges().items():
        tz = ZoneInfo(cfg["timezone"])
        local_now = now_utc.astimezone(tz)
        trade_date = local_now.date()
        if local_now.weekday() >= 5 or trade_date.isoformat() in cfg.get("holidays", []):
            continue

        close_h, close_m = map(int, cfg["close_time"].split(":"))
        close_dt = datetime.combine(trade_date, time(close_h, close_m), tzinfo=tz)
        if local_now < close_dt + SETTLE_AFTER_CLOSE:
            continue

        last = last_collected.get(code)
        if last and last >= trade_date.isoformat():
            continue
        start = (
            (datetime.fromisoformat(last).date() + timedelta(days=1))
            if last
            else trade_date
        )
        ready.append(
            {
                "exchange": code,
                "trade_date": trade_date.isoformat(),
                "start_date": start.isoformat(),
            }
        )
    return ready
