import logging
import asyncio
import tushare as ts
from common import tushare_db
from task.controller import TaskController
from datetime import datetime, timedelta, time as dtime
from task.timeutil import *
from task.common import *
from sqlalchemy.types import VARCHAR

date_fmt = '%Y-%m-%d'

logging.basicConfig(level=logging.DEBUG)

def fetch_stock_basics(conn):
    df = ts.get_stock_basics()
    df['timeToMarket'] = df['timeToMarket'].map(lambda s: datetime.strptime(str(s), '%Y%m%d') if s > 0 else None)
    df.to_sql('stock_basics', conn, if_exists="replace", dtype={"code": VARCHAR(32)}, flavor='mysql')

def incr_run():
    with tushare_db.connect() as conn:
        fetch_stock_basics(conn)
        stocks = [v for v in list(conn.execute("""SELECT code, timeToMarket from stock_basics where timeToMarket IS NOT NULL """))]
    loop = asyncio.get_event_loop()
    async def incr_stock(stock_code, start_date, task_ctrl):
        logging.debug("stock: '%s', '%s'" % (stock_code, start_date))
        last_tick = await task_ctrl.group_last("tick_%s" % stock_code)
        last_history_faa = await task_ctrl.group_last("history_faa_%s" % stock_code)
        current_time = datetime.now()
        last_tick_schedule_at = last_tick["scheduledAt"] if last_tick is not None else None
        last_history_faa_schedule_at = last_history_faa["scheduledAt"] if last_history_faa is not None else None
        t_delta = timedelta(days=1, hours=1)
        async def add_tick_task(target_date, scheduled_at):
            options = {
                "kwargs": {
                    "stock": stock_code,
                    "date": target_date.strftime(date_fmt)
                }
            }
            key = '%s_%s' % (stock_code, target_date.strftime(date_fmt))
            group = 'tick_%s' % stock_code
            return await task_ctrl.task_schedule('tick', key, scheduled_at, group=group, options=options)
        async def add_history_faa_task(start_date, end_date, scheduled_at):
            options = {
                "kwargs": {
                    "stock": stock_code,
                    "start": start_date.strftime(date_fmt),
                    "end": end_date.strftime(date_fmt)
                }
            }
            key = '%s||%s_%s' % (stock_code, start_date.strftime(date_fmt), end_date.strftime(date_fmt))
            group = 'history_faa_%s' % stock_code
            return await task_ctrl.task_schedule('history_faa', key, scheduled_at, group=group, options=options)
        async def do_history_faa():
            s = get_date(last_tick_schedule_at) if last_history_faa_schedule_at is not None else start_date
            for start, end in date_range(s, current_time, step_days=256):
                if is_terminated():
                    return
                await add_history_faa_task(start, end, end + t_delta)
        async def do_tick():
            s = get_date(last_tick_schedule_at) if last_tick_schedule_at is not None else start_date
            s = max(s, datetime.strptime("2005-01-01", date_fmt))
            for target_date, _ in date_range(s, current_time, step_days=1):
                if is_terminated():
                    return
                await add_tick_task(target_date, target_date + t_delta)
        
        await asyncio.gather(*(do_tick(), do_history_faa()))
        # await asyncio.gather(*(do_history_faa(), do_tick()))
        # await do_history_faa()
        # await do_tick()

    with TaskController.load("conf/config.yaml") as tc:
        loop.run_until_complete(asyncio.gather(*(incr_stock(code, start_date, tc) for code, start_date in stocks)))                       

incr_run()
