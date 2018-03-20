from typing import Dict, List

import arrow

from create.timestamps import events


def print_log(log: Dict) -> List:
    result = [f'{format_as_ts(log["start"])}']
    prev_ts = log['start']
    for key in events:
        if log[key] is not None and key != 'start':
            next_ts = log[key]
            result.append(
                f'{format_as_short_ts(log[key])}: {events[key]} ({format_delta(prev_ts, next_ts)})')
            prev_ts = next_ts
    return result


def format_as_human_date(date: int) -> str:
    return arrow.get(date).to(
        'local').humanize() if date is not None else ''


def format_as_date(date: int) -> str:
    return str(arrow.get(date).to('local')) if date is not None else ''


def format_as_ts(date: int) -> str:
    return arrow.get(date).to('local').strftime(
        '%A, %B %d, %H:%M:%S') if date is not None else ''


def format_as_short_ts(date: int) -> str:
    return arrow.get(date).to('local').strftime(
        '%H:%M:%S') if date is not None else ''


def format_delta(prev_ts: int, next_ts: int) -> str:
    delta = next_ts - prev_ts
    return format_seconds(delta)


def format_seconds(time: int) -> str:
    def _format_minutes(remainder: int):
        if remainder == 0:
            return ''
        return f'{remainder // 60}m ' + (
            f'{remainder % 60}s' if remainder % 60 != 0 else '')

    if time < 60:
        return f'{time}s'
    if time < 3600:
        return _format_minutes(time)
    if time <= 86400:
        hours, remainder = time // 3600, time % 3600
        return f'{hours}h ' + _format_minutes(remainder)
    if time > 86400:
        days, remainder = time // 86400, time % 86400
        hours, remainder = remainder // 3600, remainder % 3600
        return f'{days}d ' + (f'{hours}h ' if hours else ' ') + \
               _format_minutes(remainder)


def format_interval(interval: int) -> str:
    return format_seconds(interval * 60) if interval is not None else ''


def format_average_time(num: float) -> str:
    return format_seconds(round(num)) if num is not None else ''


def skip_none(text: str) -> str:
    return text if text is not None else ''
