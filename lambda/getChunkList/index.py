import os
from datetime import datetime, timedelta

def lambda_handler(event, context):
    symbol = event["symbol"]
    from_date = event["from"]
    to_date = event["to"]

    window_cfg = event.get("window", {})
    window_type = window_cfg.get("type", "days")
    window_size = int(window_cfg.get("size", 31))

    windows = split_windows(from_date, to_date, window_type, window_size)

    # Attach symbol to each window
    windows = [
        { "symbol": symbol, "from": w[0], "to": w[1] }
        for w in windows
    ]

    return { "windows": windows }


def split_windows(start, end, window_type, window_size):
    if window_type == "days":
        return split_by_days(start, end, window_size)
    if window_type == "weeks":
        return split_by_days(start, end, window_size * 7)
    if window_type == "months":
        return split_by_months(start, end)
    if window_type == "quarters":
        return split_by_quarters(start, end)

    raise ValueError(f"Unknown window type: {window_type}")


def split_by_days(start, end, days):
    windows = []
    current = datetime.fromisoformat(start)
    end_date = datetime.fromisoformat(end)

    while current <= end_date:
        window_start = current
        window_end = current + timedelta(days=days - 1)

        if window_end > end_date:
            window_end = end_date

        windows.append((window_start.date().isoformat(),
                        window_end.date().isoformat()))

        current = window_end + timedelta(days=1)

    return windows


def split_by_months(start, end):
    windows = []
    current = datetime.fromisoformat(start)
    end_date = datetime.fromisoformat(end)

    while current <= end_date:
        window_start = current

        next_month = (current.replace(day=28) + timedelta(days=4)).replace(day=1)
        window_end = next_month - timedelta(days=1)

        if window_end > end_date:
            window_end = end_date

        windows.append((window_start.date().isoformat(),
                        window_end.date().isoformat()))

        current = next_month

    return windows


def split_by_quarters(start, end):
    windows = []
    current = datetime.fromisoformat(start)
    end_date = datetime.fromisoformat(end)

    while current <= end_date:
        window_start = current

        month = current.month
        quarter_end_month = ((month - 1) // 3 + 1) * 3
        quarter_end = current.replace(month=quarter_end_month, day=1)
        quarter_end = (quarter_end.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)

        if quarter_end > end_date:
            quarter_end = end_date

        windows.append((window_start.date().isoformat(),
                        quarter_end.date().isoformat()))

        current = quarter_end + timedelta(days=1)

    return windows
