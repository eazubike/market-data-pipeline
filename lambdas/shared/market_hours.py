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
