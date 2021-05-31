# coding=utf-8
import os

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType


class SparkService:
    __conf = None
    __client = None

    def __init__(self, conf):
        self.__conf = conf
        self.__client = SparkSession.builder.appName('homework_app').getOrCreate()

    def read_from_bronze(self, file_path):
        if not self.__is_valid_bronze_path(file_path):
            raise Exception("Invalid HDFS Bronze file path")

        return self.__client.read.format('csv') \
            .option('header', True) \
            .option('multiLine', True) \
            .load(file_path)

    def __is_valid_bronze_path(self, file_path):
        if os.path.splitext(file_path)[1] == '.csv'\
                and file_path[:len(self.__conf['hdfs']['bronze_stage_dir'])] == self.__conf['hdfs']['bronze_stage_dir']:
            return True
        return False

    def save_to_silver(self, table_name, df):
        file_path = "%s/%s" % (self.__conf['hdfs']['silver_stage_dir'], table_name)
        df.write.parquet(file_path, mode="overwrite")

    def create_empty_data_frame(self, fields):
        schema = []
        for field in fields:
            schema.append(StructField(field, StringType(), True))

        return self.__client.createDataFrame(
            self.__client.sparkContext.emptyRDD(),
            StructType(schema))

    def get_bronze_table_path(self, table_name):
        return "%s/%s/%s/%s.csv" % (
            self.__conf['hdfs']['bronze_stage_dir'],
            datetime.today().strftime("%Y-%m-%d"),
            self.__conf['pg_creds']['database'],
            table_name)

