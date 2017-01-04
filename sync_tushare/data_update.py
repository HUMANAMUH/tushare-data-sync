from common import db_data

import random
import string

#table_uuid = ''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits) for _ in range(12))
table_uuid="tmp"

COLUMN_TMPL = '''
SELECT `COLUMN_NAME`
FROM `INFORMATION_SCHEMA`.`COLUMNS` 
WHERE `TABLE_SCHEMA`='{db}' 
AND `TABLE_NAME`='{table}'
'''

def get_columns(db, table):
    return COLUMN_TMPL.format(db=db, table=table)

from contextlib import contextmanager

@contextmanager
def mute_log(info_tmpl=None):
    '''
    catch except and log error, info_tmpl accept format variable {err}
    '''
    info_tmpl = info_tmpl if info_tmpl is not None else '{err}'
    try:
        yield
    except Exception as err:
        info_tmpl.format(err=err)

def history_update():
    with db_data.connect() as conn:
        with mute_log("CREATE TMP TABLE: {err}"):
            conn.execute("RENAME TABLE tushare_buffer.history TO tushare_tmp.history_{uuid}".format(uuid=table_uuid))
        with mute_log("CREATE Table: {err}"):
            conn.execute("CREATE TABLE IF NOT EXISTS tushare_data.history LIKE tushare_tmp.history_{uuid}".format(uuid=table_uuid))
            conn.execute('''ALTER TABLE tushare_data.history ADD PRIMARY KEY(stock, date)''')
            conn.execute('''ALTER TABLE tushare_data.history ADD UNIQUE INDEX date_stock (date, stock)''')
        with mute_log("INSERT RECORDS: {err}"):
            cols = ', '.join('tushare_data.history.{col}=tushare_tmp.history_{uuid}.{col}'.format(col=col[0], uuid=table_uuid) for col in conn.execute(get_columns("tushare_data", "history")) if col[0] not in {'stock', 'date'})
            conn.execute('''INSERT tushare_data.history select * from tushare_tmp.history_{uuid} ON DUPLICATE KEY UPDATE {cols}'''.format(cols=cols, uuid=table_uuid))
        with mute_log("DROP TMP Table: {err}"):
            conn.execute("DROP TABLE tushare_tmp.history_{uuid}".format(uuid=table_uuid))

def history_index_update():
    with db_data.connect() as conn:
        with mute_log("CREATE TMP TABLE: {err}"):
            conn.execute("RENAME TABLE tushare_buffer.history_index TO tushare_tmp.history_index_{uuid}".format(uuid=table_uuid))
        with mute_log("CREATE Table: {err}"):
            conn.execute("CREATE TABLE IF NOT EXISTS tushare_data.history_index LIKE tushare_tmp.history_index_{uuid}".format(uuid=table_uuid))
            conn.execute('''ALTER TABLE tushare_data.history_index ADD PRIMARY KEY(code, date)''')
            conn.execute('''ALTER TABLE tushare_data.history_index ADD UNIQUE INDEX date_stock (date, code)''')
        with mute_log("INSERT RECORDS: {err}"):
            cols = ', '.join('tushare_data.history_index.{col}=tushare_tmp.history_index_{uuid}.{col}'.format(col=col[0], uuid=table_uuid) for col in conn.execute(get_columns("tushare_data", "history_index")) if col[0] not in {'code', 'date'})
            conn.execute('''INSERT tushare_data.history_index select * from tushare_tmp.history_index_{uuid} ON DUPLICATE KEY UPDATE {cols}'''.format(cols=cols, uuid=table_uuid))
        with mute_log("DROP TMP Table: {err}"):
            conn.execute("DROP TABLE tushare_tmp.history_index_{uuid}".format(uuid=table_uuid))

    
history_update()
history_index_update()