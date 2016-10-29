import logging
import asyncio
import tushare as ts
from db import tushare_db
from task.executor import TaskExecutor
from datetime import datetime, timedelta, time as dtime

date_fmt = '%Y-%m-%d'

logging.basicConfig(level=logging.DEBUG)

def fetch_stock_basics():
    df = ts.get_stock_basics()
    df.to_sql('stock_basics', tushare_db, if_exists="replace")

def incr_run():
    #fetch_stock_basics()
    stocks = [v for v in list(tushare_db.execute("""SELECT "code", "timeToMarket" from stock_basics where "timeToMarket" > 0 """))]
    loop = asyncio.get_event_loop()
    async def incr_stock(stock_code, start_date, texecutor):
        logging.debug("stock: '%s', '%s'" % (stock_code, start_date))
        last_tick = await texecutor.group_last("tick_%s" % stock_code)
        last_history_faa = await texecutor.group_last("history_faa_%s" % stock_code)
        current_time = datetime.now()
        last_tick_schedule_at = last_tick["scheduled_at"] if last_tick is not None else None
        last_history_faa_schedule_at = last_history_faa["scheduled_at"] if last_history_faa is not None else None
        t_delta = timedelta(days=1, hours=1)
        async def add_tick_task(target_date, scheduled_at):
            options = {
                "kwargs": {
                    "stock": stock_code,
                    "date": target_date.strftime(date_fmt)
                }
            }
            return await texecutor.task_schedule('tick', '%s_%s' % (stock_code, target_date.strftime(date_fmt)), scheduled_at, group='tick_%s' % stock_code, options=options)
        async def add_history_faa_task(target_date, scheduled_at):
            options = {
                "kwargs": {
                    "stock": stock_code,
                    "start": target_date.strftime(date_fmt),
                    "end": target_date.strftime(date_fmt)
                }
            }
            return await texecutor.task_schedule('history_faa', '%s_%s' % (stock_code, target_date.strftime(date_fmt)), scheduled_at, group='history_faa_%s' % stock_code, options=options)
        async def with_date_update(last_datetime, act):
            while last_datetime is None or last_datetime.date() < (current_time + t_delta).date():
                target_date = datetime.combine(last_datetime.date(), dtime.min) if last_datetime is not None else start_date
                last_datetime = target_date + t_delta
                await act(target_date, last_datetime)
        await asyncio.gather(*(with_date_update(last_tick_schedule_at, add_tick_task), with_date_update(last_history_faa_schedule_at, add_history_faa_task)))

    with TaskExecutor.load("conf/config.yaml", loop=loop) as tx:
        loop.run_until_complete(asyncio.gather(*(incr_stock(code, datetime.strptime(str(start_date), '%Y%m%d'), tx) for code, start_date in stocks)))                       

incr_run()