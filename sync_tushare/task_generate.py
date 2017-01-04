import sys
import logging
import asyncio
import tushare as ts
from common import db_data
from task.controller import TaskController
from datetime import datetime, timedelta, time as dtime
from task.timeutil import *
from task.common import *
from sqlalchemy.types import VARCHAR

date_fmt = '%Y-%m-%d'
origin_date = datetime.strptime('1989-12-31', date_fmt)

logging.basicConfig(level=logging.DEBUG)

@with_timer(lambda t: logging.debug("Time used@fetch_stocks: %.2lfs" % t))
def fetch_stock_basics(conn):
    logging.debug("Fetch stocks")
    df = ts.get_stock_basics()
    df['timeToMarket'] = df['timeToMarket'].map(lambda s: datetime.strptime(str(s), '%Y%m%d') if s > 0 else None)
    df.to_sql('stock_basics', conn, if_exists="replace", dtype={"code": VARCHAR(32)})

@with_timer(lambda t: logging.debug("Time used@fetch indices: %.2lfs" % t))
def fetch_index_list(conn):
    logging.debug("Fetch indices")
    df = ts.get_index()[['code', 'name']]
    df.to_sql('stock_index', conn, if_exists="replace", dtype={"code": VARCHAR(32)})

loop = asyncio.get_event_loop()

async def main():
    async with TaskController.load("conf/config.yaml") as task_ctrl:
        current_time = datetime.now()
        today = get_date(current_time)
        #yesterday = today - timedelta(days=1)
        #tomorrow = today + timedelta(days=1)
        start_delay = timedelta(days=1, hours=1)

        with db_data.connect() as conn:
            fetch_stock_basics(conn)
            fetch_index_list(conn)
            stocks = [v for v in list(conn.execute("""SELECT code, timeToMarket from stock_basics where timeToMarket IS NOT NULL """))]
            stock_indices = [v[0] for v in list(conn.execute("""SELECT code from stock_index"""))]
    
        async def get_start_date(group, default=origin_date):
            last_task = await task_ctrl.group_last(group)
            last_scheduled_at = last_task["scheduledAt"] if last_task is not None else None
            return get_date(last_scheduled_at) if last_scheduled_at is not None else default

        
        async def incr_index(index_code):
            group_name = "history_index_%s" % index_code
            async def add_history_index_task(start_date, end_date, scheduled_at):
                start = start_date.strftime(date_fmt)
                end = end_date.strftime(date_fmt)
                options = {
                    "kwargs": {
                        "code": index_code,
                        "start": start,
                        "end": end
                    }
                }
                key = 'index_%s||%s_%s' % (index_code, start, end)
                group = group_name
                return await task_ctrl.task_schedule('history_index', key, scheduled_at, group=group, options=options)
            start_date = await get_start_date(group_name)
            for start, end in date_range(start_date, today, 1000):
                if is_terminated():
                    return
                await add_history_index_task(start, end, end + start_delay)
        async def incr_stock(stock_code, start_date):
            #logging.debug("stock: '%s', '%s'" % (stock_code, start_date))
            group_tick = "tick_%s" % stock_code
            group_history = "history_%s" % stock_code
            tick_start = await get_start_date(group_tick, default=datetime.strptime("2005-01-01", date_fmt))
            history_start = await get_start_date(group_history, default=start_date)
            async def add_tick_task(target_date, scheduled_at):
                options = {
                    "kwargs": {
                        "stock": stock_code,
                        "date": target_date.strftime(date_fmt)
                    }
                }
                key = 'tick_%s_%s' % (stock_code, target_date.strftime(date_fmt))
                group = 'tick_%s' % stock_code
                return await task_ctrl.task_schedule('tick', key, scheduled_at, group=group, options=options)
            async def add_history_task(start_d, end_d, scheduled_at):
                start = start_d.strftime(date_fmt)
                end = end_d.strftime(date_fmt)
                options = {
                    "kwargs": {
                        "stock": stock_code,
                        "start": start,
                        "end": end
                    }
                }
                key = 'history_%s||%s_%s' % (stock_code, start, end)
                group = 'history_%s' % stock_code
                return await task_ctrl.task_schedule('history', key, scheduled_at, group=group, options=options)

            async def do_history():
                for start, end in date_range(history_start, today, step_days=1000):
                    if is_terminated():
                        return
                    await add_history_task(start, end, end + start_delay)

            async def do_tick():
                for target_date, _ in date_range(tick_start, today, step_days=1):
                    if is_terminated():
                        return
                    await add_tick_task(target_date, target_date + start_delay)
            await do_history()
            # await do_tick()

        stock_tasks = [incr_stock(code, start_date) for code, start_date in stocks]
        index_tasks = [incr_index(code) for code in stock_indices]
        #loop.run_until_complete(asyncio.gather(*(index_tasks)))
        await asyncio.gather(*(index_tasks + stock_tasks))

loop.run_until_complete(main())
