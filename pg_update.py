import re
from sys import path
from google.protobuf.symbol_database import Default
import protolbuf
import psycopg2
import sqlparse
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT # <-- ADD THIS LINE
from sqlparse.tokens import Keyword, Name, Punctuation, String, Token, Whitespace, Literal
#tutorial for sqlparse.tokens: https://sqlparse.readthedocs.io/_/downloads/en/latest/pdf/
import pika

CLEANUP_REGEX = {
    'extract_from_para': re.compile(r'(\w)'),
    'extract_type_name' : re.compile(r"([A-Za-z0-9]+)\("),
    'exist_database' : re.compile(r"database")
}

#code block for testing regex
# regex = CLEANUP_REGEX['exist_database']
# matches = regex.search('database(2)')
# if matches:
#     print(matches)

#https://stackoverflow.com/questions/1942586/comparison-of-database-column-types-in-mysql-postgresql-and-sqlite-cross-map
#TODO: fill in map as the link above
# should be capitalized 
# TYPE_MAP = {
#     'TINYINT': 'SMALLINT',
#     'SMALLINT': 'SMALLINT'
# }

TYPE_MAP = {
    'TINYINT': 'SMALLINT',
    'SMALLINT': 'SMALLINT',
    'MEDIUMINT': 'INTEGER',
    'BIGINT': 'BIGINT',
    'BIT': 'BIT',
    'TINYINT UNSIGNED': 'SMALLINT',
    'SMALLINT UNSIGNED': 'INTEGER',
    'MEDIUMINT UNSIGNED': 'INTEGER',
    'INT UNSIGNED': 'BIGINT',
    'BIGINT UNSIGNED': 'NUMERIC(20)',
    'DOUBLE': 'DOUBLE PRECISION',
    'FLOAT': 'REAL',
    'DECIMAL': 'DECIMAL',
    'NUMERIC': 'NUMERIC',
    'BOOLEAN': 'BOOLEAN',
    'DATE': 'DATE',
    'TIME': 'TIME',
    'DATETIME': 'TIMESTAMP',
    'TIMESTAMP DEFAULT': 'TIMESTAMP DEFAULT',
    'NOW()': 'NOW()',
    'LONGTEXT': 'TEXT',
    'MEDIUMTEXT': 'TEXT',
    'BLOB': 'BYTEA',
    'VARCHAR': 'VARCHAR',
    'CHAR': 'CHAR',
    'columnname INT': 'columnname SERIAL',
    'AUTO_INCREMENT': ''
}

q = ["create database test;",
"(use test;)",
"create table if not exists `tt` (\
        id int(10) unsigned not null auto_increment,\
        `name` varchar(16) not null,\
        `sex` enum('m','w') not null default 'm',\
        `age` tinyint(3) unsigned not null,\
        `classid` char(6) default null,\
        primary key (`id`)\
        )engine=InnoDB default charset=utf8;",
"insert into tt(name,sex,age,classid) values ('yiyi','w',20,'cls1');",
"insert into tt(name,sex,age,classid) values ('xiaoer','m',22,'cls3');",
"insert into tt values (3,'zhangsan','w',21,'cls5');",
"insert into tt values (4,'lisi','m',20,'cls4');",
"insert into tt values (5,'wangwu','w',26,'cls6');",
"update tt set name='llll' where id=1;",
"delete from tt where name='llll'",
"drop table tt",
"drop database test",
"update tt set name='sss' where id=4", #q[12]
"update tt set name='ssa' where id=4" #q[13]
]

#data structure used to hold table info
class TableInfo:
    __dic = {
    'table_name':None,
    'line_info': [],
    'options' : []
    }
    #get functions
    def get_table_name(self):
        return self.__dic['table_name']
    def get_line_info(self):
        return self.__dic['line_info']
    def get_options(self):
        return self.__dic['options']

    #check functions(still consider whether it's needed)
    def check_table_info_valid(self):
        #check compulsory info exists or not
        #column info's correctness has been forced by the function itself
        return (not self.dic['table_name'])

    #modify functions
    def update_table_info(self, which_attribute, value):
        self.__dic[which_attribute] = value

    # 'insert' means insert into a list
    def insert_table_optioins(self, option):
        self.__dic['optioins'].append(
        {
            'type' : 'option_info',
            'table_options': option
        })

    def insert_column_info(self, column_name, column_type , constrains = []):
        self.__dic['line_info'].append(
            {   
                'type' : 'column_info',
                'column_name'  : column_name,
                'column_type' : column_type,
                'column_constrains' : constrains
            }
        )
    def insert_table_info(self, constrains):
        self.__dic['line_info'].append(
            {   
                'type' : 'table_info',
                'column_name'  : None,
                'column_type' : None,
                'table_constrains' : constrains
            }
        )

# for sql in q:
sql = q[2]

def split_creation_statement(sql):
    table_head, tmp = sql.split('(', 1)
    column, table_option = tmp.rsplit(')', 1)
    return table_head, column, table_option


test = ['w', '(', 'de', ')', 'de']
parenthesis_dic = {'{': '}',  '[': ']', '(': ')'}
def consolidate_function(list):
    # input a list of string to consolidate to function-like structure
    # capture xxx() like pattern
    paren_stack = []
    answer = []
    tmp = ''
    for i in list:
        tmp = tmp+i
        if i in parenthesis_dic.keys():
            paren_stack.append(i)
        elif i in parenthesis_dic.values():
            tmp = answer.pop() + tmp
            paren_stack.pop()
        if paren_stack == []:
            answer.append(tmp)
            tmp = ''
    return answer

test=['engine','=','InnoDB', 'default', 'charset', '=', 'utf8']
def cons_function(list):
    ready=False
    answer=[]
    tmp=''
    for i in list:
        tmp=tmp+i
        if i =='=':
            ready=True
        elif ready==True:
            ready=False
            answer.append(tmp)
            tmp=''
    return answer

#print(cons_function(test))

def split_into_single_line(tokens):
    # Parameter: tokens: a token list
    # function used to split contents into different lines.
    # Return value: a list whose element is a row of tokens
    line_list = []
    workbench = []
    paren_stack = []
    for token in tokens:
        if token.ttype == Whitespace:
            continue
        if token.value in parenthesis_dic.keys():
            workbench.append(token)
            paren_stack.append(token.value)
        # Definately we can check whether parenthesis is mached...
        # However, the correctness has been proved...
        elif token.value in parenthesis_dic.values():
            workbench.append(token)
            paren_stack.pop()
        elif token.value == ',':
            if len(paren_stack)  == 0 :
                line_list.append(workbench)
                workbench = []
            else:
                workbench.append(token)
        else:
            workbench.append(token)
    line_list.append(workbench)
    return line_list

def split_option(tokens):
    option_list=[]
    for token in tokens:
        if token.ttype == Whitespace:
            continue
        elif token.value == '=':
            option_list.append(token)

        else:
            option_list.append(token)

    return option_list

def extract_table_info(ctable_info, table_head_parsed_tokens):
    # extract table info from table_head_parsed_tokens and fill in ctable info
    for table_head_token in table_head_parsed_tokens:
        print(table_head_token.ttype)
        #TODO: not sure whether mandate "if not exists" every time will hamper the performance
        if table_head_token.ttype == Literal.String.Single:
            ctable_info.update_table_info('table_name', table_head_token.value)

#TODO: fill in this by create_defination in https://dev.mysql.com/doc/refman/8.0/en/create-table.html#create-table-types-attributes
# ❗ must be lowe case
#table_info_prefix = ['{', 'constraint', 'fulltext', 'primary' ]
table_info_prefix = [ 'constraint', '{fulltext}', 'primary', 'foreign', '{index}', '{key}', '{spatial}', 'unique']
def extract_line_info(ctable_info, table_head_parsed_tokens):
    # extract line information from table_head_parsed_tokens to fill in ctable_info 
    table_head_parsed_tokens = consolidate_function([i.value for i in table_head_parsed_tokens])
    if table_head_parsed_tokens[0].lower() in table_info_prefix:
        ctable_info.insert_table_info(table_head_parsed_tokens)
    else:
        # it must begin with col_name and then col_defination
        ctable_info.insert_column_info(table_head_parsed_tokens[0], table_head_parsed_tokens[1], table_head_parsed_tokens[2:])

def extract_creation_info(sql):
    #TODO: not sure what will happen if there are nesting statement...
    #replace `` with ''
    #https://wiki.postgresql.org/wiki/Things_to_find_out_about_when_moving_from_MySQL_to_PostgreSQL
    #table in mysql: https://dev.mysql.com/doc/refman/8.0/en/create-table.html#create-table-types-attributes
    #table in postgres: https://www.postgresql.org/docs/9.3/sql-createtable.html
    #TODO: use parser to replace rigid division
    sql = sql.replace('`', '\'')
    table_head, columns, table_option = split_creation_statement(sql)
    table_head_parsed_tokens = sqlparse.parse(table_head)[0]
    ctable_info = TableInfo()
    #get information from table header
    extract_table_info(ctable_info, table_head_parsed_tokens)
    #get information from lines
    columns_parsed_tokens = sqlparse.parse(columns)[0]
    columns_parsed_tokens = columns_parsed_tokens.flatten()
    lines = split_into_single_line(columns_parsed_tokens)
    for line_tokens in lines:
        extract_line_info(ctable_info, line_tokens)

    #TODO get information from table_options
    # ctable_info.insert_table_optioins()

    return ctable_info


def strip_quotes(s):
    if s[0] == '\'':
        return s.replace('\'' , '')
    elif s[0] == '`':
        return s.replace('\`' , '')
    else:
        return s


def write_column(line_info = {}):
    #generate sentence for a line
    #TODO: consider string format to make it more robust (risht now I just use nude identifier)
    # can we use '\'' to distinguish identifiers and keywords
    enum_count = 0
    before_creation_statement = []
    name = strip_quotes(line_info['column_name'])
    type = line_info['column_type']
    constrains = line_info['column_constrains']
    regex = CLEANUP_REGEX['extract_type_name']
    plain_type = regex.search(type).group(1)

    #update constrains
    new_constrains = []
    for constrain in constrains:
        #conflict constrains:
        if constrain.lower() == 'auto_increment':
            type = 'SERIAL'
        elif constrain.lower() == 'unsigned':
            #https://stackoverflow.com/questions/20810134/why-unsigned-integer-is-not-available-in-postgresql
            # TODO: add domain as suggested
            continue
        else:#there are no conflicts between constrains
            new_constrains.append(constrain)

    #conflict data type
    #show types:https://stackoverflow.com/questions/9535937/is-there-a-way-to-show-a-user-defined-postgresql-enumerated-type-definition
    #https://stackoverflow.com/questions/10923213/postgres-enum-data-type-or-check-constraint
    #choose method 1
    #TODO: To make it more robust, consider check if the type has exist, if it exists, drop old one and create new one.
    if plain_type == 'enum':
        constrains.insert(0, '')
        tmp = 'CREATE TYPE {} AS {};'.format('auto_generated_enum'+str(enum_count), type)
        type = 'auto_generated_enum'+str(enum_count)
        before_creation_statement.append(tmp)
        enum_count += 1
    if plain_type.strip().upper() in TYPE_MAP.keys():
        type = TYPE_MAP[plain_type.strip().upper()]

    return '{name} {type} {constrains},'.format(
        name = name,
        type = type,
        constrains = " ".join(new_constrains)
    ), before_creation_statement


def write_table_constrain(table_info = {}):
    # generate 
    ret = ''
    for i in table_info['table_constrains']:
        ret += i.replace('\'', '') + ' '
    ret = ret+','
    return ret

def write_create_table(table_info):
    # generate pg sentence
    pre_statements = []
    pg_sql = 'CREATE TABLE {} ( '.format(strip_quotes(table_info.get_table_name()))
    for i in table_info.get_line_info():
        if i['type'] == 'column_info':
            pg_sql = pg_sql + write_column(i)[0]
            pre_statements.extend(write_column(i)[1])
        elif i['type'] == 'table_info':
            pg_sql = pg_sql + write_table_constrain(i)
    pg_sql = pg_sql[:-1]
    pg_sql = pg_sql + ');'
    # TODO: write table option
    return (pg_sql, pre_statements)

def receive():

    credentials=pika.PlainCredentials('software', '123456')
    connection=pika.BlockingConnection(
        pika.ConnectionParameters(host='192.168.21.173', port=5672, credentials=credentials))
    channel=connection.channel()
    # 申明消息队列，消息在这个队列传递，如果不存在，则创建队列
    channel.queue_declare(queue='data-trans', durable=False)

    # 定义一个回调函数来处理消息队列中的消息，这里是打印出来
    def callback(ch, method, properties, body):
        ch.basic_ack(delivery_tag=method.delivery_tag)
        print(body.decode())
        return body.decode()

    # 告诉rabbitmq，用callback来接收消息
    channel.basic_consume('data-trans', callback)
    # 开始接收信息，并进入阻塞状态，队列里有信息才会调用callback进行处理
    channel.start_consuming()


#test entry
sql = q[11]
#sql = q[0]
#sql = q[2]
message = None
database = "use `test`"
#not my work, just use this code block to generate protobuf
regex = CLEANUP_REGEX['exist_database']
matches = regex.search(sql)
if matches:
        protolbuf.analy(database = sql, sql = '')
        message = receive()
        message_type = message.DESCRIPTOR.full_name
        thedb = 'postgres'
elif sql[:15] == "(use test;)":
    protolbuf = None

else:
    protolbuf.analy(database, sql)
    message = protolbuf.get_mess()
    message_type = message.DESCRIPTOR.full_name
    thedb = message.database
    thedb = thedb.replace('`', '')

print(message_type)



con = psycopg2.connect(
    host = "localhost",
    database = thedb,
    user = "postgres",
    password = "123456"
)
con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT) # <-- ADD THIS LINE to make sure we can manipulate db
cur = con.cursor()


if message_type == 'createDBMess':
    #TODO: optimize these code, such as not commit each execute to promise consistency
    #I got inspiration from here https://stackoverflow.com/questions/18389124/simulate-create-database-if-not-exists-for-postgresql
    #creation option 1: create a schema instead of database
    #reference:

    # print('creating schema')
    # tmp_sql = 'CREATE SCHEMA IF NOT EXISTS {}'.format(message.database)
    # cur.execute(tmp_sql)
    # con.commit()
    # print('successfuly created schema')
    # print('creating database')
    # tmp_sql = 'DROP DATABASE IF EXISTS {};'.format(message.database)
    # cur.execute(tmp_sql)
    # con.commit()

    #creation option 2: create a database directly, if database has exist, just connect to this db
    show_sql = 'SELECT * FROM pg_database'
    cur.execute(show_sql)
    current_databases = [i[0] for i in cur.fetchall()]
    print(current_databases)
    thedb = message.database[:-1]
    if thedb in current_databases:
        print('db already exists, simply connect to db')
        pass #TODO
    else:
        tmp_sql = 'CREATE DATABASE {};'.format(thedb)
        cur.execute(tmp_sql)
        con.commit()
    con = psycopg2.connect(
    host = "localhost",
    database = thedb,
    user = "postgres",
    password = "123456")
    print('successfuly created db')
elif message_type == 'createTableMess':
    #nothing special, consider "create table if not exists"
    print('creating table')
    table_info = extract_creation_info(sql)
    pg_sql, pre_pg_sql = write_create_table(table_info)
    print(pg_sql)
    cur.execute(pre_pg_sql[0])
    con.commit()
    cur.execute(pg_sql)
    con.commit()
    print('successfuly created a table')
        # tmp_sql = 'CREATE TABLE IF NOT EXISTS{} ('.format(sql.table)
    # tmp_sql = tmp_sql + 
elif message_type == 'insertMessWithAttr':
    #have yet find conflicts, test to decide whether rewrite it
    print('inserting')
    cur.execute(message.sql)
    con.commit()
elif message_type == 'insertMess':
    print('inserting')
    cur.execute(message.sql)
    con.commit()
    print('success!')
elif message_type == 'deleteMess':
    print('deleting')
    cur.execute(message.sql)
    con.commit()
    print('success!')
elif message_type == 'updateMess':
    print('updating')
    cur.execute(message.sql)
    con.commit()
    print('success!')
elif message_type == 'dropTableMess':
    print('droping table')
    cur.execute(message.sql)
    con.commit()
elif message_type == 'dropDBMess':
    #https://stackoverflow.com/questions/17449420/postgresql-unable-to-drop-database-because-of-some-auto-connections-to-db/17461681
    print('droping database')
    thedb = message.database
    revoke_connection_sql = 'REVOKE CONNECT ON DATABASE {} FROM public;'.format(thedb)
    cur.execute(revoke_connection_sql)
    terminate_connection_sql = 'select pg_terminate_backend(pid) from pg_stat_activity where DATNAME = \'{}\';'.format(thedb)
    cur.execute(terminate_connection_sql)
    tmp_sql = 'DROP DATABASE {};'.format(thedb)
    cur.execute(tmp_sql)
    con.commit()
    print('successfuly dropped db')
cur.close()
con.close()

#if __name__ == '__main__':