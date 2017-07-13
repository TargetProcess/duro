import asyncio
import configparser
from datetime import datetime as dt

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.date import DateTrigger
from concurrent.futures import ProcessPoolExecutor


from create.sqlite import get_tables_to_create
from create import create


def find_and_run_new_jobs(db_path: str):
    sub_scheduler = BackgroundScheduler()
    print(dt.now())
    # table = get_tables_to_create(db_path)[0]
    # sub_scheduler.add_job(create, args=[table[0], bool(table[1])],
    #                       trigger=DateTrigger(run_date=dt.now()))
    for table in get_tables_to_create(db_path)[:2]:
        print(table)
        sub_scheduler.add_job(create, args=[table[0], bool(table[1])],
                              trigger=DateTrigger(run_date=dt.now()))
    sub_scheduler.start()

if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('config.conf')
    db_path = config['main'].get('db', './duro.db')

    scheduler = AsyncIOScheduler()
    scheduler.add_job(find_and_run_new_jobs, 'interval', seconds=60, args=[db_path])
    # scheduler.add_job(find_and_run_new_jobs, trigger=DateTrigger(run_date=dt.now()),
    #                   args=[db_path])
    scheduler.start()

    try:
        asyncio.get_event_loop().run_forever()
    except (KeyboardInterrupt, SystemExit):
        pass
