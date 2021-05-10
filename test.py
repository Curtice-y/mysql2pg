import pymysql
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    WriteRowsEvent,
    UpdateRowsEvent,
    DeleteRowsEvent,
)


mysql_setting = {'host': '127.0.0.1', 'port': 3306, 'user': 'root', 'passwd': 'yfyyfy'}

stream = BinLogStreamReader(connection_settings=mysql_setting, server_id=1)

for binlog_event in stream:
    binlog_event.dump()
stream.close()
