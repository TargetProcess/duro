import time

import arrow
import pytest

from create.timestamps import Timestamps


# noinspection PyUnresolvedReferences
def test_timestamps():
    ts = Timestamps()
    events = ['start', 'connect', 'select',
              'create_temp', 'process', 'csv',
              's3', 'insert', 'clean_csv', 'tests',
              'replace_old', 'drop_old', 'finish']
    assert ts.__slots__ == events
    assert ts.events == events

    now = arrow.now().timestamp
    ts.log('start')
    assert ts.start - now <= 1
    ts.log('csv')
    assert ts.csv - now <= 1
    with pytest.raises(AttributeError):
        ts.log('random')

    values = [v for v in ts.values if v is not None]
    assert len(values) == 2
    assert ts.duration is None

    time.sleep(2)
    ts.log('drop_old')
    assert ts.drop_old - now <= 3
    assert ts.finish - now <= 3
    assert ts.duration <= 3
