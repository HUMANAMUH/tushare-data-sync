import logging
import asyncio
import pandas as pd
import tushare as ts
from sqlalchemy import TIMESTAMP
from common import tushare_db
from task.executor import TaskExecutor
from task.timeutil import *
from datetime import datetime
import time

loop = asyncio.get_event_loop()
tx = TaskExecutor.load("conf/config.yaml", loop=loop, multi_process=True)

time_fmt = '%Y-%m-%d %H:%M:%S'

def logtime(key):
    return lambda t: logging.debug("%s: %.3f", key, t)

@tx.register("tick", expand_param=True)
def fetch_tick(stock, date):
    with timer(logtime("get_tick_data")):
        df = ts.get_tick_data(stock, date=date)
    if df is None:
        logging.debug("no tick data for stock: ts.get_tick_data('%s', date='%s')" % (stock, date))
        return
    if len(df) > 0 and "当天没有数据" in df['time'][0]:
        # no data found
        logging.debug("no tick data for stock: ts.get_tick_data('%s', date='%s')" % (stock, date))
        return
    with timer(logtime("tick_data_proc")):
        df['stock'] = stock
        df['time'] = (date + ' ' + df['time']).map(lambda x: pd.Timestamp(datetime.strptime(x, time_fmt), tz='Asia/Shanghai').strftime(format="%Y-%m-%d %H:%M:%S%z"))
        ans = df.set_index(['stock', 'time'])
    with tushare_db.connect() as conn:
        try:
            with timer(logtime("tick_data_del")):
                del_sql = """delete from tick_data where "stock"='%s' AND "time" >= timestamp '%s' AND "time" < timestamp '%s' + interval '1 day' """ % (stock, date, date) 
                logging.debug(del_sql)
                conn.execute(del_sql)
        except:
            pass
        logging.debug("data got: ts.get_tick_data('%s', date='%s')" % (stock, date))
        with timer(logtime("tick_data_append")):
            ans.to_sql('tick_data', conn, if_exists="append", dtype={'time': TIMESTAMP(timezone=True)})


@tx.register("history_faa", expand_param=True)
def fetch_history_faa(stock, start, end):
    """
    History data forward answer authority
    """
    with timer(logtime("get_history_faa")):
        df = ts.get_h_data(stock, autype='hfq', start=start, end=end)
    if df is None:
        logging.debug("no history data for stock: ts.get_h_data('%s', autype='hfq', start='%s', end='%s')" % (stock, start, end))
        return
    with timer(logtime("history_faa_proc")):
        df['stock'] = stock
        ans = df.reset_index().set_index(['stock','date'])
    with tushare_db.connect() as conn:
        try:
            with timer(logtime("history_faa_del")):
                del_sql = """delete from history_faa where "stock"='%s' AND "date" >= timestamp '%s' AND "date" <= timestamp '%s'""" % (stock, start, end)
                logging.debug(del_sql)
                conn.execute(del_sql)
        except:
            pass
        logging.debug("data got: ts.get_h_data('%s', autype='hfq', start='%s', end='%s')" % (stock, start, end))
        with timer(logtime("history_faa_append")):
            ans.to_sql('history_faa', conn, if_exists="append")

logging.basicConfig(level=logging.DEBUG)
loop.run_until_complete(tx.run())
tx.close()