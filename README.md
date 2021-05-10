
安装mysqlreplication库: pip install mysql-replication
pymysql = 0.9.3


usage:python main.py [--start-file START_FILE] [--stop-file END_FILE] [--help]
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
