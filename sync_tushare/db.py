from sqlalchemy import create_engine

#tushare_db = create_engine('postgresql+pg8000://earthson@localhost/trading_tushare', pool_size=128, max_overflow=0)
tushare_db = create_engine('postgresql+psycopg2://earthson@localhost/trading_tushare', pool_size=128, max_overflow=0)
# tushare_db = create_engine('mysql+pymysql://data:showmethemoney@localhost/tushare', pool_size=128, pool_recycle=400, max_overflow=0)