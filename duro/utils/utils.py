from typing import Optional


def convert_interval_to_integer(interval: Optional[str]) -> Optional[int]:
    if interval is None:
        return None
    units = {"m": 1, "h": 60, "d": 1440, "w": 10080}
    unit = interval[-1].lower()
    if unit not in units.keys():
        raise ValueError("Invalid interval")

    try:
        value = int(interval[:-1])
        return value * units[unit]
    except ValueError:
        raise ValueError("Invalid interval")
