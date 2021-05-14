import sys
import pymysql
from pymysqlreplication import BinLogStreamReader
from method import command_line_args
import threading
from protolbuf import analy

class Binlog2sql(object):
    def __init__(self, connection_settings, start_file=None, end_file=None):
        """
        :param connection_settings: 连接设置
        :param start_file: 开始文件
        :param end_file: 结束文件 读取到end_file的前一个文件, end_file的读取使用process_binlog函数
        :parameter databaseList 存储历史sql操作的对应数据库
        :parameter sqlListHis 存储历史sql语句 与databaseList在位置上一一对应
        :parameter current 实时读取end_file
        """
        self.conn_setting = connection_settings
        self.start_file = start_file
        self.end_file = end_file

        self.binlogList = []
        self.connection = pymysql.connect(**self.conn_setting)

        self.databaseList = []
        self.sqlListHis = []
        self.current = []
        #
        with self.connection as cursor:
            # 获取末尾
            cursor.execute("SHOW MASTER STATUS")
            # cursor.fetchone(): ('file', position(int), '', '', '')
            # 一次读取一条数据, 读取后游标自动移向下一条
            # print(cursor.fetchone())
            self.eof_file, self.eof_pos = cursor.fetchone()[:2]
            # print(self.eof_file, self.eof_pos)
            cursor.execute("SHOW MASTER LOGS")
            # 读取所有binlog文件名
            bin_index = [row[0] for row in cursor.fetchall()]
            if self.start_file==None:
                self.start_file = bin_index[0]
            if self.end_file == "":
                self.end_file = self.eof_file
            # 'mysql-bin.000001' -> '000001'
            binlog2i = lambda x: x.split('.')[1]
            for binary in bin_index:
                if binlog2i(self.start_file) <= binlog2i(binary) <= binlog2i(self.end_file):
                    self.binlogList.append(binary)
            # 获取server_id
            cursor.execute('SELECT @@server_id')
            self.server_id = cursor.fetchone()[0]
            if not self.server_id:
                raise ValueError('missing server_id in %s:%s' %(self.conn_setting['host'], self.conn_setting['port']))
            # print(self.binlogList)

            self.databaseList = []
            self.sqlListHis = []
            query_base = '''show binlog events in \"'''
            for file in self.binlogList:
                query = query_base + file + '''\"'''
                cursor.execute(query)
                for row in cursor:
                    col = row[5].split(';')
                    first = col[0].split(' ')
                    # print(first)
                    if first[0] == 'use':
                        sql = col[1].split(' ')
                        # print(sql)
                        if sql[1] != 'flush':
                            self.databaseList.append(col[0])
                            self.sqlListHis.append(col[1])
                    elif first[0] == 'drop':
                        self.databaseList.append(col[0])
                        self.sqlListHis.append('')
                    elif first[0] == 'create':
                        self.databaseList.append(col[0])
                        self.sqlListHis.append('')
            # print(self.databaseList)
            # print(self.sqlListHis)
            for database, sql in zip(self.databaseList, self.sqlListHis):
                # print(database)
                # print(sql)
                analy(database, sql)


    def process_binlog(self):
        """
        :parameter last_events 标志是否读取到最后一个事件
        :parameter 允许读取的MySQL语句
        """
        last_events = False
        allow_sql = ['update', 'delete', 'insert', 'CREATE', 'DROP']
        stream = BinLogStreamReader(connection_settings=self.conn_setting, server_id=1)
        for binlog_events in stream:
            if binlog_events.event_type == 2:
                # 读取query语句
                query = binlog_events.query
                # 空格分割 获取命令query[0]
                query_ = query.split(' ')
                # print(query_)
                if query_[0] in allow_sql:
                    self.current.append(binlog_events.query)
                print(self.current)
            # 读取到最后一个事件
            if stream.log_file == self.end_file and stream.log_pos == self.eof_pos:
                last_events = True
            # 不退出循环表示获取实时mysql语句
            # if last_events == True: break
        stream.close()
        print(self.current)



if __name__ == '__main__':
    # 安装: pip install mysql-replication
    # pymysql = 0.9.3

    # 获取命令行输入 sys.argv sys.argv[0]为python 舍弃
    """
    usage: main.py [--start-file START_FILE] [--stop-file END_FILE] [--help]
               [-h HOST] [-u USER] [-p [PASSWORD [PASSWORD ...]]] [-P PORT]

    Parse MySQL binlog to SQL you want

    optional arguments:
    --start-file START_FILE
                        Start binlog file to be parsed (required)
    --stop-file END_FILE, --end-file END_FILE
                        Stop binlog file to be parsed
    --help              help information
    -h HOST, --host HOST  Host the MySQL database server located
    -u USER, --user USER  MySQL Username to log in as
    -p [PASSWORD [PASSWORD ...]], --password [PASSWORD [PASSWORD ...]]
                        MySQL Password to use (required)
    start_file 和 stop_file 要写在password前面, 不要加单引号
    例: --start-file mysql-bin.000001 ✔
        --start-file 'mysql-bin.000001' ❌
    """
    args = command_line_args(sys.argv[1:])
    conn_setting = {'host': args.host, 'port': args.port, 'user': args.user, 'passwd': args.password, 'charset': 'utf8'}
    # 创建Binlog2sql对象 self.__init__()中完成历史mysql提取
    binlog2sql = Binlog2sql(connection_settings=conn_setting, start_file=args.start_file, end_file=args.end_file)
    # 实时获取mysql
    # binlog2sql.process_binlog()
