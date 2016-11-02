import logging
import asyncio
import pandas as pd
import tushare as ts
from sqlalchemy import TIMESTAMP
from common import tushare_db
from task.executor import TaskExecutor
from datetime import datetime
import time

loop = asyncio.get_event_loop()
tx = TaskExecutor.load("conf/config.yaml", loop=loop, multi_process=True)

time_fmt = '%Y-%m-%d %H:%M:%S'

@tx.register("tick", expand_param=True)
def fetch_tick(stock, date):
    df = ts.get_tick_data(stock, date=date)
    if df is None:
        logging.debug("no tick data for stock: ts.get_tick_data('%s', date='%s')" % (stock, date))
        return
    if len(df) > 0 and "当天没有数据" in df['time'][0]:
        # no data found
        logging.debug("no tick data for stock: ts.get_tick_data('%s', date='%s')" % (stock, date))
        return
    df['stock'] = stock
    df['time'] = (date + ' ' + df['time']).map(lambda x: pd.Timestamp(datetime.strptime(x, time_fmt), tz='Asia/Shanghai').strftime(format="%Y-%m-%d %H:%M:%S%z"))
    ans = df.set_index(['stock', 'time'])
    with tushare_db.connect() as conn:
        try:
            conn.execute("""delete from tick_data where "stock"='%s' AND "time"::date = date '%s' """ % (stock, date))
        except:
            pass
        logging.debug("data got: ts.get_tick_data('%s', date='%s')" % (stock, date))
        ans.to_sql('tick_data', conn, if_exists="append", dtype={'time': TIMESTAMP(timezone=True)})


@tx.register("history_faa", expand_param=True)
def fetch_history_faa(stock, start, end):
    """
    History data forward answer authority
    """
    df = ts.get_h_data(stock, autype='hfq', start=start, end=end)
    if df is None:
        logging.debug("no history data for stock: ts.get_h_data('%s', autype='hfq', start='%s', end='%s')" % (stock, start, end))
        return
    df['stock'] = stock
    ans = df.reset_index().set_index(['stock','date'])
    with tushare_db.connect() as conn:
        try:
            conn.execute("""delete from history_faa where "stock"='%s' AND "date">='%s' AND "date"<='%s'""" % (stock, start, end))
        except:
            pass
        logging.debug("data got: ts.get_h_data('%s', autype='hfq', start='%s', end='%s')" % (stock, start, end))
        ans.to_sql('history_faa', conn, if_exists="append")

logging.basicConfig(level=logging.DEBUG)
loop.run_until_complete(tx.run())
tx.close()