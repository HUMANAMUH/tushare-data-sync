import logging
import asyncio
import tushare as ts
from db import tushare_db
from task.executor import TaskExecutor
from datetime import datetime, timedelta

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
        last_tick = await texecutor.group_last("tick_%s" % stock_code)
        last_history_faa = await texecutor.group_last("history_faa_%s" % stock_code)
        current_time = datetime.now()
        last_tick_schedule_at = last_tick["scheduled_at"] if last_tick is not None else None
        last_history_faa_schedule_at = last_history_faa["scheduled_at"] if last_history_faa is not None else None
        t_delta = timedelta(days=1, hours=1)
        d_delta = timedelta(days=1)
        while last_tick_schedule_at is None or last_tick_schedule_at.date() < (current_time + t_delta).date():
            target_date = last_tick_schedule_at.date()
            last_tick_schedule_at = target_date + t_delta
            await texecutor.task_schedule('tick', '%s_%s' % (stock_code, target_date.strftime(date_fmt)), last_tick_schedule_at, group='tick_%s' % stock_code)
        while last_history_faa_schedule_at is None or last_history_faa_schedule_at.date() < (current_time + t_delta).date():
            target_date = last_history_faa_schedule_at.date()
            last_history_faa_schedule_at = target_date + t_delta
            await texecutor.task_schedule('history_faa', '%s_%s' % (stock_code, target_date.strftime(date_fmt)), last_history_faa_schedule_at)

    with TaskExecutor.load("conf/config.yaml", loop=loop) as tx:
        loop.run_until_complete(asyncio.gather(*(incr_stock(code, datetime.strptime(str(start_date), '%Y%m%d'), tx) for code, start_date in stocks)))                       

incr_run()