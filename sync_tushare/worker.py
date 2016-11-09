import logging
import asyncio
import pandas as pd
import tushare as ts
from sqlalchemy import DATETIME
from sqlalchemy.types import VARCHAR
from common import tushare_db
from task.executor import TaskExecutor
from task.timeutil import *
from datetime import datetime
import time
from task.common import *
from task import event_loop
from task.data_buffer import BufferedDataProcessor
from concurrent.futures import ProcessPoolExecutor

tx = TaskExecutor.load("conf/config.yaml", multi_process=False)

proc_pool = ProcessPoolExecutor(max_workers=64)
tick_buffer = BufferedDataProcessor(num_worker=1)
history_buffer = BufferedDataProcessor(num_worker=1)

def logtime(key):
    return lambda t: logging.debug("%s: %.3f", key, t)

@tick_buffer.on_combine
@history_buffer.on_combine
def df_merge(a, b):
    return pd.concat([a, b])

@tick_buffer.processor
@with_timer(logtime("tick_data_append"))
def tick_insert(data):
    logging.debug("tick_data_rows: %d", len(data))
    dtypes = { k: VARCHAR(32) for k, v in data.dtypes.items() if v.name == 'object'}
    dtypes['time'] = DATETIME(timezone=True)
    with tushare_db.connect() as conn:
        try:
            print("###")
            print(data)
            data.to_sql('tick_data', conn, if_exists="append", index=False, dtype=dtypes, chunksize=None, flavor='mysql')
        except Exception as e:

            raise e

@history_buffer.processor
@with_timer(logtime("history_data_append"))
def history_insert(data):
    logging.debug("history_data_rows: %d", len(data))
    dtypes = { k: VARCHAR(32) for k, v in data.dtypes.items() if v.name == 'object'}
    with tushare_db.connect() as conn:
        data.to_sql('history_faa', conn, index=False, if_exists="append", dtype=dtypes, chunksize=None, flavor='mysql')

time_fmt = '%Y-%m-%d %H:%M:%S'


@tx.register("tick", expand_param=True)
async def fetch_tick(stock, date):
    df = await wait_concurrent(event_loop, proc_pool, ts.get_tick_data, stock, date=date, pause=1)
    if df is None:
        logging.debug("no tick data for stock: ts.get_tick_data('%s', date='%s')" % (stock, date))
        return
    if len(df) > 0 and "当天没有数据" in df['time'][0]:
        # no data found
        logging.debug("no tick data for stock: ts.get_tick_data('%s', date='%s')" % (stock, date))
        return
    # with timer(logtime("tick_data_proc")):
    df['stock'] = stock
    df['time'] = (date + ' ' + df['time']).map(lambda x: pd.Timestamp(datetime.strptime(x, time_fmt), tz='Asia/Shanghai').strftime(format="%Y-%m-%d %H:%M:%S%z"))
    return tick_buffer.proc_data(df)
    # with tushare_db.connect() as conn:
    #     try:
    #         with timer(logtime("tick_data_del")):
    #             del_sql = """delete from tick_data where "stock"='%s' AND "time" >= timestamp '%s' AND "time" < timestamp '%s' + interval '1 day' """ % (stock, date, date) 
    #             logging.debug(del_sql)
    #             conn.execute(del_sql)
    #     except Exception as e:
    #         logging.warn(e)
    #         pass
    #     logging.debug("data got: ts.get_tick_data('%s', date='%s')" % (stock, date))
    #     with timer(logtime("tick_data_append")):
    #         ans.to_sql('tick_data', conn, if_exists="append", dtype={'time': TIMESTAMP(timezone=True)})


@tx.register("history_faa", expand_param=True)
async def fetch_history_faa(stock, start, end):
    """
    History data forward answer authority
    """
    #with timer(logtime("get_history_faa")):
    df = await wait_concurrent(event_loop, proc_pool, ts.get_h_data, stock, autype='hfq', start=start, end=end, pause=1)
    if df is None:
        logging.debug("no history data for stock: ts.get_h_data('%s', autype='hfq', start='%s', end='%s')" % (stock, start, end))
        return
    # with timer(logtime("history_faa_proc")):
    df['stock'] = stock
    ans = df.reset_index()
    return history_buffer.proc_data(ans)
    # with tushare_db.connect() as conn:
    #     try:
    #         with timer(logtime("history_faa_del")):
    #             del_sql = """delete from history_faa where "stock"='%s' AND "date" >= timestamp '%s' AND "date" <= timestamp '%s'""" % (stock, start, end)
    #             logging.debug(del_sql)
    #             conn.execute(del_sql)
    #     except Exception as e:
    #         logging.warn(e)
    #         pass
    #     logging.debug("data got: ts.get_h_data('%s', autype='hfq', start='%s', end='%s')" % (stock, start, end))
    #     with timer(logtime("history_faa_append")):
    #         ans.to_sql('history_faa', conn, if_exists="append")

logging.basicConfig(level=logging.DEBUG)
tx.run()
tick_buffer.run()
history_buffer.run()
event_loop.run_until_complete(wait_all_task_done())
print("Exit")
tx.close()
tick_buffer.close()
history_buffer.close()