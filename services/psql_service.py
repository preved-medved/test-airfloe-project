# coding=utf-8
import psycopg2


class PsqlService:
    __conf = None
    __connection = None

    def __init__(self, conf):
        self.__conf = conf
        self.__connection = psycopg2.connect(**self.__conf['pg_creds'])

    def dump_to_csv_file(self, file_path, table):
        query = 'COPY public.%s TO STDOUT WITH HEADER CSV' % table

        cursor = self.__connection.cursor()
        with open(file=file_path, mode='w') as csv_file:
            cursor.copy_expert(query, csv_file)
