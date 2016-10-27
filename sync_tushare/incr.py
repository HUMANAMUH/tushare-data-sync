import logging
import tushare as ts
from db import tushare_db
from task.executor import TaskExecutor
from datetime import datetime, timedelta


logging.basicConfig(level=logging.DEBUG)

def fetch_stock_basics():
    df = ts.get_stock_basics()
    df.to_sql('stock_basics', tushare_db, if_exists="replace")

def incr_run():
    #fetch_stock_basics()
    stick_codes = [v[0] for v in list(tushare_db.execute("SELECT code,  from stock_basics"))]
    loop = asyncio.get_event_loop()
    async def incr_stock(stock_code, start_date, texecutor):
        last_tick = await texecutor.group_last("tick_%s" % stock_code)
        last_stock_faa = await texecutor.group_last("stock_faa_%s" % stock_code)
        current_time = datetime.now()

    with TaskExecutor.load("conf/config.yaml", loop=loop) as tx:
        for stick_code in stick_codes:
            tx.group_last()
        

incr_run()