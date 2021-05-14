from pymysql.constants.COMMAND import COM_BINLOG_DUMP, COM_REGISTER_SLAVE
from pymysql.cursors import DictCursor
from pymysql.util import int2byte

def aa():
    conn_setting = {'host': '127.0.0.1', 'port': 3306, 'user': 'root', 'passwd': 'yfyyfy', 'charset': 'utf8'}
    _stream_connection = pymysql_wrapper(**conn_settings)

    if True:
        # only when log_file and log_pos both provided, the position info is
        # valid, if not, get the current position from master
        if log_file is None:
            cur = _stream_connection.cursor()
            cur.execute("SHOW MASTER STATUS")
            master_status = cur.fetchone()
            if master_status is None:
                raise BinLogNotEnabled() # error handle
            log_file, log_pos = master_status[:2]
            cur.close()