

def build_single_schedule(table: str, interval: str) -> str:
    return '''- name: Set up cron for %(table)s
        cron:
            name=%(table)s
            user="{{ project.user }}"
            %(schedule)s
            job="cd {{ project.app }}
            && . {{ virtualenv.path }}/bin/activate
            && python3 create_table.py {table}
            ''' % {'table': table, 'schedule': create_ansible_schedule(interval)}


def create_ansible_schedule(interval_string):
    if interval_string in ('1w', '7d'):
        return 'weekday="6" hour="3"'

    amount, unit = convert_to_correct_unit(int(interval_string[:-1]), interval_string[-1])
    expanded_names = {'m': 'minute', 'h': 'hour', 'd': 'day'}
    interval = f'/{amount}' if amount != 1 else ''
    result = f'{expanded_names[unit]}="*{interval}"'
    if unit == 'd':
        result += ' hour="4" minute="30"'
    if unit == 'h':
        result += ' minute="10"'
    return result


def convert_to_correct_unit(value, time_unit):
    maximums = {'m': 60, 'h': 24, 'd': 7, 'w': 2}
    units = 'mhdw'
    max_value = maximums[time_unit]
    if value < max_value:
        return value, time_unit
    elif value % max_value == 0:
        try:
            next_unit = units[units.find(time_unit) + 1]
            return value // max_value, next_unit
        except IndexError:
            raise ValueError('Incorrect time period')
    else:
        raise ValueError('Incorrect time period')