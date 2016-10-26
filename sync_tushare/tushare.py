import tushare as ts
from sqlalchemy import create_engine

tushare_db = create_engine('postgresql://earthson@localhost/trading_tushare', pool_size=32, max_overflow=0)

def fetch_stock_basics():
    df = ts.get_stock_basics()
    ans = df.set_index(['code'])
    ans.to_sql('stock_basics', tushare_db, if_exists="replace")

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

#History data forward answer authority
def fetch_history_faa(stock, start, end):
    df = ts.get_h_data(stock, autype='hfq', start=start, end=end)
    df['stock'] = stock
    ans = df.set_index(['stock','date'])
    try:
        tushare_db.execute("""delete from history_faa where "stock"='%s' AND "date">='%s' AND "date"<='%s'""" % (stock, start, end))
    except:
        pass
    ans.to_sql('history_faa', tushare_db, if_exists="append")