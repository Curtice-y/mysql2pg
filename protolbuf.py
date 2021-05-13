import re
import trans_pb2
import pika

# temp.ParseFromString(message) 反序列化
destination = '127.0.0.1'
port = 5672
user = 'software'
passwd = '154231'
queue_name = 'data-trans'

def format_str(str):
    str = str.replace("'", "")
    str = str.replace("`", "")
    return str

def send(message):
    print(message)
    # message = message.SerializeToString()
    # credentials = pika.PlainCredentials(user, passwd)
    # connection = pika.BlockingConnection(
    #     pika.ConnectionParameters(host=destination, port=port, credentials=credentials))
    # channel = connection.channel()
    #
    # data = channel.queue_declare(queue=queue_name)
    # channel.basic_publish(exchange='', routing_key=queue_name, body=message)

def insertAnaly(sql, database):
    sql_split = re.split('values', sql)
    # get table name
    table = re.search(r'insert into[\s]*([\w|_]*)', sql_split[0]).group(1).strip()
    attributes = re.search(r'\((.*)\)', sql_split[0])
    sql_one = re.findall(r'\(([^\)]*)\)', sql_split[1])
    if attributes:
        attributes = attributes.group(1).split(',')
        for i in range(len(attributes)):
            attributes[i] = format_str(attributes[i])
        for i in sql_one:
            message = trans_pb2.insertMessWithAttr()
            message.command = 'insert'
            message.database = database
            message.table = table
            values = re.split(',', i)
            for j in range(len(values)):
                if re.search('\'', values[j]):
                    values[j] = format_str(values[j])
                temp = message.attribute.add()
                temp.key = attributes[j]
                temp.value = values[j]
            send(message)
    else:
        for i in sql_one:
            message = trans_pb2.insertMess()
            message.command = 'insert'
            message.database = database
            message.table = table
            values = re.split(',', i)
            for j in range(len(values)):
                if re.search('\'', values[j]):
                    values[j] = format_str(values[j])
                message.values.append(values[j])
            send(message)

def whereAnaly(sql, message):
    sql_limit = sql.split('where')[1]
    logic = re.findall(r'and|or', sql_limit)
    for i in logic:
        message.where.logic.append(i)
    limit_one = re.split(r'and|or', sql_limit)
    for i in limit_one:
        limit = message.where.limits.add()
        temp = re.search(r'[\s]*([^\s]*)(<|>|<=|>=|=|!=)([^\s]*)', i)
        limit.left = temp.group(1)
        limit.operand = temp.group(2)
        limit.right = temp.group(3)
        limit.left = format_str(limit.left)
        limit.right = format_str(limit.right)



def updateAnaly(sql, database):
    sql_split = re.split('set', sql)
    table = re.search(r'update\s*([\w|_]*)', sql_split[0]).group(1).strip()
    message = trans_pb2.updateMess()
    message.command = 'update'
    message.database = database
    message.table = table

    sql_one = re.split(',', sql_split[1])
    for i in sql_one:
        temp = message.attribute.add()
        k_v = re.search(r'\s*([^\s]*)=([^\s]*)', i)
        temp.key = k_v.group(1)
        value = k_v.group(2)
        if re.search('\'', value):
            value = format_str(value)
        temp.value = value
    if re.search('where', sql_split[1]):
        whereAnaly(sql_split[1], message)
    send(message)

def deleteAnaly(sql, database):
    table = re.search(r'delete from\s*([\w|_]*)', sql).group(1).strip()
    message = trans_pb2.deleteMess()
    message.command = 'delete'
    message.database = database
    message.table = table
    if re.search('where', sql):
        whereAnaly(sql, message)
    send(message)

def createTableAnaly(sql, database):
    table = re.search(r'\s*([^\s]*)\s*\(', sql).group(1).strip()
    table = format_str(table)
    message = trans_pb2.createTableMess()
    message.command = 'create table'
    message.database = database
    message.table = table

    attributes = re.search(r'\((.*)\)', sql, re.S).group(1)
    attributes = attributes.split(',\n')
    for i in attributes:
        i = i.strip()
        message.defines.append(i)
    send(message)


def dropTableAnaly(sql, database):
    table = re.search(r'drop table\s*([\w|_]*)', sql).group(1).strip()
    message = trans_pb2.dropTableMess()
    message.command = 'drop table'
    message.database = database
    message.table = table
    send(message)


def analy(database, sql):
    database = database.split(' ')
    sql = sql.lower()
    if len(database) == 3:
        db_name = database[2]
        if database[0] == 'create':
            message = trans_pb2.createDBMess()
            message.command = 'create database'
            message.database = db_name
        elif database[0] == 'drop':
            message = trans_pb2.dropDBMess()
            message.command = 'drop database'
            message.database = db_name
        send(message)
    elif len(database) == 2:
        db_name = database[1]
        command = re.search(r'\s*([^\s]*)\s*', sql).group(1)
        if command == 'insert':
            insertAnaly(sql, db_name)
        elif command == 'update':
            updateAnaly(sql, db_name)
        elif command == 'delete':
            deleteAnaly(sql, db_name)
        elif command == 'create':
            createTableAnaly(sql, db_name)
        elif command == 'drop':
            dropTableAnaly(sql, db_name)



