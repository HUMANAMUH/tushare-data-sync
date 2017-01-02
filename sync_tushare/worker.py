import logging
import asyncio
import pandas as pd
import tushare as ts
from sqlalchemy import DATETIME
from sqlalchemy.types import VARCHAR
from common import db_buffer
from task.executor import TaskExecutor
from task.timeutil import *
from datetime import datetime
import time
from task.common import *
from task import event_loop
from task.data_buffer import BufferedDataProcessor
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

def do_nothing(*args, **kwargs):
    pass

def disable_stdout():
    import os
    import sys
    f = open(os.devnull, 'w')
    sys.stdout = f
    sys.stdout.flush = do_nothing
    sys.stdout.write = do_nothing

tx = TaskExecutor.load("conf/config.yaml", multi_process=False)

proc_pool = ProcessPoolExecutor(max_workers=64)
#proc_pool = ThreadPoolExecutor(max_workers=64)
tick_buffer = BufferedDataProcessor(num_worker=4)
history_buffer = BufferedDataProcessor(num_worker=8)
history_index_buffer = BufferedDataProcessor(num_worker=1)

def logtime(key):
    return lambda t: logging.debug("%s: %.3fs", key, t)

@tick_buffer.on_combine
@history_buffer.on_combine
def df_merge(a, b):
    return pd.concat([a, b])

@tick_buffer.processor
@with_timer(logtime("tick_data_append"))
def tick_insert(data):
    logging.debug("tick_data_rows: %d", len(data))
    dtypes = { k: VARCHAR(32) for k, v in data.dtypes.items() if v.name == 'object'}
    dtypes['time'] = DATETIME
    with db_buffer.connect() as conn:
        data.to_sql('tick_data', conn, if_exists="append", index=False, dtype=dtypes, chunksize=None)

@history_buffer.processor
@with_timer(logtime("history_data_append"))
def history_insert(data):
    logging.debug("history_data_rows: %d", len(data))
    dtypes = { k: VARCHAR(32) for k, v in data.dtypes.items() if v.name == 'object'}
    with db_buffer.connect() as conn:
        data.to_sql('history', conn, index=False, if_exists="append", dtype=dtypes, chunksize=None)

@history_index_buffer.processor
@with_timer(logtime("history_index_data_append"))
def history_index_insert(data):
    logging.debug("history_index_data_rows: %d", len(data))
    dtypes = { k: VARCHAR(32) for k, v in data.dtypes.items() if v.name == 'object'}
    with db_buffer.connect() as conn:
        data.to_sql('history_index', conn, index=False, if_exists="append", dtype=dtypes, chunksize=None)

time_fmt = '%Y-%m-%d %H:%M:%S'


@tx.register("tick", expand_param=True)
async def fetch_tick(stock, date):
    with timer(logtime("ts.get_tick_data('%s', date='%s')" % (stock, date))):
        df = await wait_concurrent(event_loop, proc_pool, ts.get_tick_data, stock, date=date, pause=0.1)
    if df is None or (len(df) > 0 and "当天没有数据" in df['time'][0]):
        # no data found
        logging.debug("no tick data for stock: ts.get_tick_data('%s', date='%s')" % (stock, date))
        return
    # with timer(logtime("tick_data_proc")):
    df['stock'] = stock
    df['time'] = (date + ' ' + df['time']).map(lambda x: pd.Timestamp(datetime.strptime(x, time_fmt)).strftime(format="%Y-%m-%d %H:%M:%S%z"))
    return tick_buffer.proc_data(df)


@tx.register("history", expand_param=True)
async def fetch_history(stock, start, end):
    """
    History data forward answer authority
    """
    disable_stdout()
    with timer(logtime("ts.get_h_data('%s', autype=None, start='%s', end='%s', drop_factor=False)" % (stock, start, end))):
        df = await wait_concurrent(event_loop, proc_pool, ts.get_h_data, stock, autype=None, start=start, end=end, drop_factor=False, pause=0.05)
    if df is None:
        logging.debug("no history data for stock: ts.get_h_data('%s', autype=None, start='%s', end='%s')" % (stock, start, end))
        return
    df['stock'] = stock
    ans = df.reset_index()
    return history_buffer.proc_data(ans)

@tx.register("history_index", expand_param=True)
async def fetch_history_index(code, start, end):
    disable_stdout()
    info = "ts.get_h_data('%s', start='%s', end='%s', pause=0.05, index=True)" % (code, start, end)
    with timer(logtime(info)):
        df = await wait_concurrent(event_loop, proc_pool, ts.get_h_data, code, start=start, end=end, pause=0.05, index=True)
    if df is None:
        logging.debug("no history index data for code: %s", info)
        return
    df['code'] = code
    ans = df.reset_index()
    return history_index_buffer.proc_data(ans)

logging.basicConfig(level=logging.DEBUG)
tx.run()
tick_buffer.run()
history_buffer.run()
history_index_buffer.run()
event_loop.run_until_complete(wait_all_task_done())
print("Exit")
tx.close()
tick_buffer.close()
history_buffer.close()
