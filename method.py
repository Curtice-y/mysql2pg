import sys
import argparse
import datetime
import getpass

def is_valid_datetime(string):

    try:
        datetime.datetime.strptime(string, "%Y-%m-%d %H:%M:%S")
        return True
    except:
        return False

def parse_args():
    # 解析命令行输入
    parser = argparse.ArgumentParser(description='Parse MySQL binlog to SQL you want', add_help=False)
    # interval
    parser.add_argument('--start-file', dest='start_file', type=str, help='Start binlog file to be parsed')
    parser.add_argument('--stop-file', '--end-file', dest='end_file', type=str, help="Stop binlog file to be parsed", default='')
    parser.add_argument('--help', dest='help', action='store_true', help='help information', default=False)
    # 连接设置
    parser.add_argument('-h', '--host', dest='host', type=str, help='Host the MySQL database server located', default='127.0.0.1')
    parser.add_argument('-u', '--user', dest='user', type=str, help='MySQL Username to log in as', default='root')
    parser.add_argument('-p', '--password', dest='password', type=str, nargs='*', help='MySQL Password to use', default='')
    parser.add_argument('-P', '--port', dest='port', type=int, help='MySQL port to use', default=3306)
    # schema
    parser.add_argument('-d', '--databases', dest='databases', type=str, nargs='*', help='dbs you want to process', default='')
    parser.add_argument('-t', '--tables', dest='tables', type=str, nargs='*', help='tables you want to process', default='')

    return parser


def command_line_args(args):
    need_print_help = False if args else True
    parser = parse_args()
    args = parser.parse_args(args)
    if args.help or need_print_help:
        parser.print_help()
        sys.exit(1)
    if not args.password:
        args.password = getpass.getpass()
    else:
        args.password = args.password[0]
    return args
