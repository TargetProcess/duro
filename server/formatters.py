from typing import Dict, List, Optional, Tuple

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


def format_as_human_date(date: Optional[int]) -> str:
    return arrow.get(date).to(
        'local').humanize() if date is not None else ''


def format_as_date(date: Optional[int]) -> str:
    return str(arrow.get(date).to('local')) if date is not None else ''


def format_as_ts(date: Optional[int]) -> str:
    return arrow.get(date).to('local').strftime(
        '%A, %B %d, %H:%M:%S') if date is not None else ''


def format_as_short_ts(date: Optional[int]) -> str:
    return arrow.get(date).to('local').strftime(
        '%H:%M:%S') if date is not None else ''


def format_delta(prev_ts: int, next_ts: int) -> str:
    delta = next_ts - prev_ts
    return format_seconds(delta)


def format_minutes(seconds: Optional[int]):
    if not seconds:
        return ''
    minutes, remainder = seconds // 60, seconds % 60
    if not remainder:
        return f'{minutes}m'
    if not minutes:
        return f'{remainder}s'
    return f'{minutes}m {remainder}s'


def format_seconds(time: Optional[int]) -> str:
    if time is None:
        return ''
    if time < 60:
        return f'{time}s'
    if time < 3600:
        return format_minutes(time)
    if time <= 86400:
        hours, remainder = time // 3600, time % 3600
        if not remainder:
            return f'{hours}h'
        return f'{hours}h {format_minutes(remainder)}'

    days, remainder = time // 86400, time % 86400
    if not remainder:
        return f'{days}d'
    hours, remainder = remainder // 3600, remainder % 3600
    if not remainder:
        return f'{days}d {hours}h'

    return f'{days}d {hours}h {format_minutes(remainder)}'


def format_interval(minutes: Optional[int]) -> str:
    return format_seconds(minutes * 60) if minutes is not None else ''


def format_average_time(num: Optional[float]) -> str:
    return format_seconds(round(num)) if num is not None else ''


def skip_none(text: Optional[str]) -> str:
    return text if text is not None else ''


def format_job(job) -> Dict:
    start = arrow.get(job['start']).format()
    finish = arrow.get(job['finish']).format() if job['finish'] else None

    return {'table': job['table'],
            'start': start,
            'finish': finish}


def prepare_table_details(details: List) -> Tuple[List, List]:
    if len(details) == 1 and details[0]['start'] is None:
        return [], []
    return ([print_log(d) for d in details],
            [{'date': arrow.get(d['start']).format(),
              'duration': d['finish'] - d['start']}
             for d in details])
