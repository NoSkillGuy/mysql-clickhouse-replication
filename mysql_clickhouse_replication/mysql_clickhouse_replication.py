import sys
import datetime
import pymysql
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import QueryEvent, RotateEvent, FormatDescriptionEvent
from .mysql_clickhouse_replication_util import command_line_args, concat_sql_from_binlog_event, create_unique_file, temp_open, is_dml_event, event_type
import pymysql.cursors
from pprint import pprint
from clickhouse_driver import Client
import os
from mysql_clickhouse_replication.tablesqlbuilder import TableSQLBuilder

class Mysql2clickhousesql(object):

    def __init__(self, 
                 connection_settings, 
                 start_file=None, 
                 start_pos=None, 
                 only_schemas=None, 
                 only_tables=None,  
                 stop_never=False, 
                 only_dml=True, 
                 sql_type=None
                 ):

        if not start_file:
            raise ValueError('Lack of parameter: start_file')

        self.conn_setting = connection_settings
        self.start_file = start_file
        self.start_pos = start_pos if start_pos else 4
        self.only_schemas = only_schemas if only_schemas else None
        self.only_tables = only_tables if only_tables else None
        self.stop_never = stop_never
        self.only_dml = only_dml
        self.sql_type = [t.upper() for t in sql_type] if sql_type else []

        self.binlogList = []
        self.connection = pymysql.connect(**self.conn_setting)
        with self.connection as cursor:
            cursor.execute("SHOW MASTER STATUS")
            self.eof_file, self.eof_pos = cursor.fetchone()[:2]
            cursor.execute("SHOW MASTER LOGS")
            bin_index = [row[0] for row in cursor.fetchall()]
            if self.start_file not in bin_index:
                raise ValueError('parameter error: start_file %s not in mysql server' % self.start_file)
            binlog2i = lambda x: x.split('.')[1]
            for binary in bin_index:
                if binlog2i(self.start_file) <= binlog2i(binary):
                    self.binlogList.append(binary)

            cursor.execute("SELECT @@server_id")
            self.server_id = cursor.fetchone()[0]
            if not self.server_id:
                raise ValueError('missing server_id in %s:%s' % (self.conn_setting['host'], self.conn_setting['port']))

    def process_binlog(self):
        pprint(vars(self))
        stream = BinLogStreamReader(connection_settings=self.conn_setting,
                                    server_id=self.server_id,
                                    log_file=self.start_file, 
                                    log_pos=self.start_pos, 
                                    only_schemas=self.only_schemas,
                                    only_tables=self.only_tables, 
                                    resume_stream=True, 
                                    blocking=True)
        e_start_pos, last_pos = stream.log_pos, stream.log_pos

        tmp_file = create_unique_file('%s.%s' % (self.conn_setting['host'], self.conn_setting['port']))
        with temp_open(tmp_file, "w") as f_tmp, self.connection as cursor:

            for binlog_event in stream:

                if isinstance(binlog_event, QueryEvent) and binlog_event.query == 'BEGIN':
                    e_start_pos = last_pos

                if isinstance(binlog_event, QueryEvent) and not self.only_dml:
                    sql = concat_sql_from_binlog_event(cursor=cursor, binlog_event=binlog_event)
                    if sql:
                        self.run_sql_on_clickhouse(sql)
                elif is_dml_event(binlog_event) and event_type(binlog_event) in self.sql_type:
                    for row in binlog_event.rows:
                        sql = concat_sql_from_binlog_event(cursor=cursor, binlog_event=binlog_event, row=row, e_start_pos=e_start_pos)
                        self.run_sql_on_clickhouse(sql)

                if not (isinstance(binlog_event, RotateEvent) or isinstance(binlog_event, FormatDescriptionEvent)):
                    last_pos = binlog_event.packet.log_pos

            stream.close()
            f_tmp.close()
        return True

    def __del__(self):
        pass

    def run_sql_on_clickhouse(self, sql):
        client = Client('localhost')
        print(sql)
        clickhouse_sql = "clickhouse-client -h 127.0.0.1 --query=\"{}\"".format(sql).replace('`','')
        print(clickhouse_sql)
        os.system(clickhouse_sql)
        # client.execute(sql)
        


def callbinlog2clickhousesql(connection_settings=None, 
                             start_file=None, 
                             start_pos=None,
                             only_schemas=None, 
                             only_tables=None,
                             stop_never=False, 
                             only_dml=True, 
                             sql_type=None):
    binlog2clickhousesql = Mysql2clickhousesql(connection_settings=connection_settings, 
                                                start_file=start_file, 
                                                start_pos=start_pos,
                                                only_schemas=only_schemas, 
                                                only_tables=only_tables,
                                                stop_never=stop_never,
                                                only_dml=only_dml, 
                                                sql_type=sql_type,
                                                )
    binlog2clickhousesql.process_binlog()

def main():
    args = command_line_args(sys.argv[1:])
    conn_setting = {
            'host': args.host, 
            'port': args.port, 
            'user': args.user, 
            'passwd': args.password, 
            'charset': 'utf8'
            }
    callbinlog2clickhousesql(connection_settings=conn_setting, 
                             start_file=args.start_file, 
                             start_pos=args.start_pos,
                             only_schemas=args.databases, 
                             only_tables=args.tables,
                             stop_never=args.stop_never,
                             only_dml=args.only_dml, 
                             sql_type=args.sql_type,
                             )

