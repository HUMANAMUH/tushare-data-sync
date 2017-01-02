import yaml
from sqlalchemy import create_engine

with open("conf/config.yaml", "r") as fobj:
    config = yaml.load(fobj.read())["tushare"]


db_buffer = create_engine(config["db_buffer"], pool_size=64, pool_recycle=400, max_overflow=0)
db_tmp = create_engine(config["db_tmp"], pool_size=64, pool_recycle=400, max_overflow=0)
db_data = create_engine(config["db_data"], pool_size=64, pool_recycle=400, max_overflow=0)