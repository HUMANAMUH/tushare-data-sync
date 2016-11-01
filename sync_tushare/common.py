import yaml
from sqlalchemy import create_engine

with open("conf/config.yaml", "r") as fobj:
    config = yaml.load(fobj.read())["tushare"]

#tushare_db = create_engine('postgresql+pg8000://earthson@localhost/trading_tushare', pool_size=128, max_overflow=0)
tushare_db = create_engine(config["db_uri"], pool_size=16, max_overflow=0)
# tushare_db = create_engine('mysql+pymysql://data:showmethemoney@localhost/tushare', pool_size=128, pool_recycle=400, max_overflow=0)