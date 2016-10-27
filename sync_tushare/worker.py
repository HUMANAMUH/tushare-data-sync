import asyncio
import tushare as ts
from db import tushare_db
from task.executor import TaskExecutor

loop = asyncio.get_event_loop()
tx = TaskExecutor.load("conf/config.yaml", loop=loop)

@tx.arg_register("tick")
def fetch_tick(stock, date):
    df = ts.get_tick_data(stock, date=date)
    df['stock'] = stock
    df['date'] = date
    ans = df.set_index(['stock', 'date'])
    try:
        tushare_db.execute("""delete from tick_data where "stock"='%s' AND "date"='%s' """ % (stock, date))
    except:
        pass
    ans.to_sql('tick_data', tushare_db, if_exists="append")


@tx.arg_register("history_faa")
def fetch_history_faa(stock, start, end):
    """
    History data forward answer authority
    """
    df = ts.get_h_data(stock, autype='hfq', start=start, end=end)
    df['stock'] = stock
    ans = df.set_index(['stock','date'])
    try:
        tushare_db.execute("""delete from history_faa where "stock"='%s' AND "date">='%s' AND "date"<='%s'""" % (stock, start, end))
    except:
        pass
    ans.to_sql('history_faa', tushare_db, if_exists="append")

loop.run_until_complete(tx.run())
tx.close()