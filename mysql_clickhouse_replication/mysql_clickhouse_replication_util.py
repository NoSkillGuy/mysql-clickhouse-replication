import os
import sys
import argparse
import datetime
import getpass
from contextlib import contextmanager
from pymysqlreplication.event import QueryEvent
from pymysqlreplication.row_event import (
    WriteRowsEvent,
    UpdateRowsEvent,
    DeleteRowsEvent,
)
from pprint import pprint

if sys.version > '3':
    PY3PLUS = True
else:
    PY3PLUS = False

def is_valid_datetime(string):
    try:
        datetime.datetime.strptime(string, "%Y-%m-%d %H:%M:%S")
        return True
    except:
        return False


def create_unique_file(filename):
    version = 0
    result_file = filename
    while os.path.exists(result_file) and version < 1000:
        result_file = filename + '.' + str(version)
        version += 1
    if version >= 1000:
        raise OSError('cannot create unique file %s.[0-1000]' % filename)
    return result_file


@contextmanager
def temp_open(filename, mode):
    f = open(filename, mode)
    try:
        yield f
    finally:
        f.close()
        os.remove(filename)


def parse_args():
    parser = argparse.ArgumentParser(description='use Mysql binlogs to replicate to clickhouse', add_help=False)
    
    mysql_connect_setting = parser.add_argument_group('Mysql connect setting')
    mysql_connect_setting.add_argument('-mh', '--mysql_host', dest='mysql_host', type=str,
                                 help='Host the MySQL database server located', default='127.0.0.1')
    mysql_connect_setting.add_argument('-mu', '--mysql_user', dest='mysql_user', type=str,
                                 help='MySQL Username to log in as', default='root')
    mysql_connect_setting.add_argument('-mp', '--mysql_password', dest='mysql_password', type=str, nargs='*',
                                 help='MySQL Password to use', default='')
    mysql_connect_setting.add_argument('-mP', '--mysql_port', dest='mysql_port', type=int,
                                 help='MySQL port to use', default=3306)
    mysql_connect_setting.add_argument('-mc', '--mysql_charset', dest='mysql_charset', type=str,
                                 help='MySQL charset to use', default='utf8')

    clikhouse_connect_setting = parser.add_argument_group('Clickhouse connect setting')
    clikhouse_connect_setting.add_argument('-ch', '--clickhouse_host', dest='clickhouse_host', type=str,
                                 help='Host the Clickhouse database server located', default='127.0.0.1')
    clikhouse_connect_setting.add_argument('-cu', '--clickhouse_user', dest='clickhouse_user', type=str,
                                 help='Clickhouse Username to log in as', default='root')
    clikhouse_connect_setting.add_argument('-cp', '--clickhouse_password', dest='clickhouse_password', type=str, nargs='*',
                                 help='Clickhouse Password to use', default='')
    clikhouse_connect_setting.add_argument('-cP', '--clickhouse_port', dest='clickhouse_port', type=int,
                                 help='Clickhouse port to use', default=9000)
    
    interval = parser.add_argument_group('interval filter')
    interval.add_argument('--start-file', dest='start_file', type=str, help='Start binlog file to be parsed')
    interval.add_argument('--start-position', '--start-pos', dest='start_pos', type=int,
                          help='Start position of the --start-file', default=4)

    parser.add_argument('--stop-never', dest='stop_never', action='store_true', default=False,
                        help="Continuously parse binlog. default: stop at the latest event when you start.")
    parser.add_argument('--help', dest='help', action='store_true', help='help information', default=False)

    schema = parser.add_argument_group('schema filter')
    schema.add_argument('-d', '--databases', dest='databases', type=str, nargs='*',
                        help='dbs you want to process', default='')
    schema.add_argument('-t', '--tables', dest='tables', type=str, nargs='*',
                        help='tables you want to process', default='')

    event = parser.add_argument_group('type filter')
    event.add_argument('--only-dml', dest='only_dml', action='store_true', default=False,
                       help='only print dml, ignore ddl')
    event.add_argument('--sql-type', dest='sql_type', type=str, nargs='*', default=['INSERT', 'UPDATE', 'DELETE', 'CREATE', 'ALTER' ],
                       help='Sql type you want to process, support INSERT, UPDATE, DELETE, CREATE, ALTER.')

    parser.add_argument('-K', '--no-primary-key', dest='no_pk', action='store_true',
                        help='Generate insert sql without primary key if exists', default=False)
    return parser


def command_line_args(args):
    need_print_help = False if args else True
    parser = parse_args()
    args = parser.parse_args(args)
    if args.help or need_print_help:
        parser.print_help()
        sys.exit(1)
    if not args.mysql_user:
        args.mysql_user = getpass.getuser(prompt='MySQL Username: ')

    if not args.mysql_password:
        args.mysql_password = getpass.getpass(prompt='MySQL Password: ')
    else:
        args.mysql_password = args.mysql_password[0]

    if not args.clickhouse_user:
        args.clickhouse_user = getpass.getuser(prompt='ClickHouse Username: ')

    if not args.clickhouse_password:
        args.clickhouse_password = getpass.getpass(prompt='ClickHouse Password: ')
    else:
        args.clickhouse_password = args.clickhouse_password[0]
    return args


def compare_items(items):
    (k, v) = items
    if v is None:
        return '`%s` IS %%s' % k
    else:
        return '`%s`=%%s' % k


def fix_object(value):
    """Fixes python objects so that they can be properly inserted into SQL queries"""
    if isinstance(value, set):
        value = ','.join(value)
    if PY3PLUS and isinstance(value, bytes):
        return value.decode('utf-8')
    elif not PY3PLUS and isinstance(value, unicode):
        return value.encode('utf-8')
    else:
        return value


def is_dml_event(event):
    if isinstance(event, WriteRowsEvent) or isinstance(event, UpdateRowsEvent) or isinstance(event, DeleteRowsEvent):
        return True
    else:
        return False


def event_type(event):
    t = None
    if isinstance(event, WriteRowsEvent):
        t = 'INSERT'
    elif isinstance(event, UpdateRowsEvent):
        t = 'UPDATE'
    elif isinstance(event, DeleteRowsEvent):
        t = 'DELETE'
    return t


def concat_sql_from_binlog_event(cursor, binlog_event, row=None, e_start_pos=None):

    if not (isinstance(binlog_event, WriteRowsEvent) or isinstance(binlog_event, UpdateRowsEvent) or isinstance(binlog_event, DeleteRowsEvent) or isinstance(binlog_event, QueryEvent)):
        raise ValueError('binlog_event must be WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent or QueryEvent')

    sql = ''
    if isinstance(binlog_event, WriteRowsEvent) or isinstance(binlog_event, UpdateRowsEvent) or isinstance(binlog_event, DeleteRowsEvent):
        pattern = generate_sql_pattern(binlog_event, row=row)
        sql = cursor.mogrify(pattern['template'], pattern['values'])
    elif isinstance(binlog_event, QueryEvent) and binlog_event.query != 'BEGIN' and binlog_event.query != 'COMMIT':
        pprint(binlog_event)
        pprint(vars(binlog_event))
        split_binlog_event_query = binlog_event.query.split(' ')
        if split_binlog_event_query[0] == 'create':
            if split_binlog_event_query[1] == 'database':
                sql += '{0};'.format(fix_object(binlog_event.query))
                # sql += create_sql_query(sql)
            # elif split_binlog_event_query[1] == 'table':
                # sql += '{0};'.format(fix_object(binlog_event.query))
                # sql += create_sql_query('select 1')
        elif split_binlog_event_query[0] == 'truncate':
            sql += 'truncate table `{0}`.`{1}`;'.format(binlog_event.schema.recv(1024).decode(),fix_object(binlog_event.query).split(' ')[1])            
            # sql += create_sql_query(sql)

            # sql = create_sql_query(sql)

        # if binlog_event.schema:
        #     sql = 'USE {0};\n'.format(binlog_event.schema)
        # sql += '{0};'.format(fix_object(binlog_event.query))

    return sql

# def create_sql_query(sql, binlog_event, type_of_event):
#     if type_of_event == 'truncate':
#         sql += '{0};'.format(fix_object(binlog_event.query))
#     else:
#         sql += '{0};'.format(fix_object(binlog_event.query))
#     return sql

def generate_sql_pattern(binlog_event, row=None):
    print(binlog_event)
    template = ''
    values = []
    if isinstance(binlog_event, WriteRowsEvent):
        template = 'INSERT INTO `{0}`.`{1}`({2}) VALUES ({3});'.format(
            binlog_event.schema, binlog_event.table,
            ', '.join(map(lambda key: '`%s`' % key, row['values'].keys())),
            ', '.join(['%s'] * len(row['values']))
        )
        values = map(fix_object, row['values'].values())
    elif isinstance(binlog_event, DeleteRowsEvent):
        template = 'ALTER TABLE `{0}`.`{1}` DELETE WHERE {2};'.format(
            binlog_event.schema, binlog_event.table, ' AND '.join(map(compare_items, row['values'].items())))
        values = map(fix_object, row['values'].values())
    elif isinstance(binlog_event, UpdateRowsEvent):
        if binlog_event.primary_key:
           row['after_values'].pop(binlog_event.primary_key)
           row['before_values'].pop(binlog_event.primary_key)
        template = 'ALTER TABLE `{0}`.`{1}` UPDATE {2} WHERE {3};'.format(
            binlog_event.schema, binlog_event.table,
            ', '.join(['`%s`=%%s' % k for k in row['after_values'].keys()]),
            ' AND '.join(map(compare_items, row['before_values'].items()))
        )
        values = map(fix_object, list(row['after_values'].values())+list(row['before_values'].values()))



    return {'template': template, 'values': list(values)}
