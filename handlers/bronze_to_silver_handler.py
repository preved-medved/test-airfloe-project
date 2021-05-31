# coding=utf-8
import os
import logging
from services.hdfs_service import HdfsService
from services.config_service import get_config
from services.spark_service import SparkService

from pyspark.sql.types import IntegerType, DateType
import pyspark.sql.functions as F
from datetime import datetime


def bronze_to_silver(func):
    def inner():
        config = get_config()
        spark = SparkService(config)
        hdfs = HdfsService(config)

        func(config, spark, hdfs)

    return inner


@bronze_to_silver
def import_orders_table(config, spark, hdfs):
    file_path = spark.get_bronze_table_path('orders')
    if not hdfs.is_file_exist(file_path):
        raise Exception('The file "%s" does not exist!' % file_path)

    df = spark.read_from_bronze(file_path)

    func = F.udf(lambda x: datetime.strptime(x, '%Y-%m-%d'), DateType())

    df = df.withColumn("order_id", df["order_id"].cast(IntegerType())) \
        .withColumn("product_id", df["product_id"].cast(IntegerType())) \
        .withColumn("client_id", df["client_id"].cast(IntegerType())) \
        .withColumn("store_id", df["store_id"].cast(IntegerType())) \
        .withColumn("quantity", df["quantity"].cast(IntegerType())) \
        .withColumn('order_date', func(df['order_date']))

    invalid_row_count = df.where(
        (df["order_id"] < 1)
        | (df["product_id"] < 1)
        | (df["client_id"] < 1)
        | (df["store_id"] < 1)).count()

    if invalid_row_count > 0:
        logging.warning('The table "orders" have invalid values')

    df = df.groupBy(
        F.col("order_id"),
        F.col("product_id"),
        F.col("client_id"),
        F.col("store_id"),
        F.col("quantity"),
        F.col("order_date")) \
        .count() \
        .drop(F.col("count"))

    spark.save_to_silver('orders', df)
    logging.info('The table "orders" uploaded to bronze!')


@bronze_to_silver
def import_clients_table(config, spark, hdfs):
    file_path = spark.get_bronze_table_path('clients')
    if not hdfs.is_file_exist(file_path):
        raise Exception('The file "%s" does not exist!' % file_path)

    df = spark.read_from_bronze(file_path)

    df = df.withColumn("id", df["id"].cast(IntegerType())) \
        .withColumn("location_area_id", df["location_area_id"].cast(IntegerType()))

    invalid_row_count = df.where(
        (df["id"] < 1)
        | (df["location_area_id"] < 1)).count()
    if invalid_row_count > 0:
        logging.warning('The table "clients" have invalid values')

    spark.save_to_silver('clients', df)
    logging.info('The table "clients" uploaded to bronze!')


@bronze_to_silver
def import_location_areas_table(config, spark, hdfs):
    file_path = spark.get_bronze_table_path('location_areas')
    if not hdfs.is_file_exist(file_path):
        raise Exception('The file "%s" does not exist!' % file_path)

    df = spark.read_from_bronze(file_path)

    df = df.withColumn("area_id", df["area_id"].cast(IntegerType()))

    invalid_row_count = df.where((df["area_id"] < 1)).count()
    if invalid_row_count > 0:
        logging.warning('The table "location_areas" have invalid values')

    spark.save_to_silver('location_areas', df)
    logging.info('The table "location_areas" uploaded to bronze!')


@bronze_to_silver
def import_products_table(config, spark, hdfs):
    file_path = spark.get_bronze_table_path('products')
    if not hdfs.is_file_exist(file_path):
        raise Exception('The file "%s" does not exist!' % file_path)

    df = spark.read_from_bronze(file_path)

    df = df.withColumn("product_id", df["product_id"].cast(IntegerType())) \
        .withColumn("aisle_id", df["aisle_id"].cast(IntegerType())) \
        .withColumn("department_id", df["department_id"].cast(IntegerType()))

    invalid_row_count = df.where(
        (df["product_id"] < 1)
        | (df["aisle_id"] < 1)
        | (df["department_id"] < 1)).count()
    if invalid_row_count > 0:
        logging.warning('The table "products" have invalid values')

    spark.save_to_silver('products', df)
    logging.info('The table "products" uploaded to bronze!')


@bronze_to_silver
def import_store_types_table(config, spark, hdfs):
    file_path = spark.get_bronze_table_path('store_types')
    if not hdfs.is_file_exist(file_path):
        raise Exception('The file "%s" does not exist!' % file_path)

    df = spark.read_from_bronze(file_path)

    df = df.withColumn("store_type_id", df["store_type_id"].cast(IntegerType()))

    invalid_row_count = df.where((df["store_type_id"] < 1)).count()
    if invalid_row_count > 0:
        logging.warning('The table "store_types" have invalid values')

    spark.save_to_silver('store_types', df)
    logging.info('The table "store_types" uploaded to bronze!')


@bronze_to_silver
def import_stores_table(config, spark, hdfs):
    file_path = spark.get_bronze_table_path('stores')
    if not hdfs.is_file_exist(file_path):
        raise Exception('The file "%s" does not exist!' % file_path)

    df = spark.read_from_bronze(file_path)

    df = df.withColumn("store_id", df["store_id"].cast(IntegerType())) \
        .withColumn("location_area_id", df["location_area_id"].cast(IntegerType())) \
        .withColumn("store_type_id", df["store_type_id"].cast(IntegerType()))

    invalid_row_count = df.where(
        (df["store_id"] < 1)
        | (df["location_area_id"] < 1)
        | (df["store_type_id"] < 1)).count()
    if invalid_row_count > 0:
        logging.warning('The table "stores" have invalid values')

    df = df.groupBy(F.col("store_id"), F.col("location_area_id"), F.col("store_type_id")) \
        .count() \
        .drop(F.col("count"))

    spark.save_to_silver('stores', df)
    logging.info('The table "stores" uploaded to bronze!')


@bronze_to_silver
def import_departments_table(config, spark, hdfs):
    file_path = spark.get_bronze_table_path('departments')
    if not hdfs.is_file_exist(file_path):
        raise Exception('The file "%s" does not exist!' % file_path)

    df = spark.read_from_bronze(file_path)

    df = df.withColumn("department_id", df["department_id"].cast(IntegerType()))

    invalid_row_count = df.where((df["department_id"] < 1)).count()
    if invalid_row_count > 0:
        logging.warning('The table "departments" have invalid values')

    spark.save_to_silver('departments', df)
    logging.info('The table "departments" uploaded to bronze!')


@bronze_to_silver
def import_aisles_table(config, spark, hdfs):
    file_path = spark.get_bronze_table_path('aisles')
    if not hdfs.is_file_exist(file_path):
        raise Exception('The file "%s" does not exist!' % file_path)

    df = spark.read_from_bronze(file_path)

    df = df.withColumn("aisle_id", df["aisle_id"].cast(IntegerType()))

    invalid_row_count = df.where((df["aisle_id"] < 1)).count()
    if invalid_row_count > 0:
        logging.warning('The table "aisles" have invalid values')

    spark.save_to_silver('aisles', df)
    logging.info('The table "aisles" uploaded to bronze!')

@bronze_to_silver
def import_out_of_stock_table(config, spark, hdfs):
    config = get_config()
    spark = SparkService(config)
    hdfs = HdfsService(config)

    hdfs_file_path = os.path.join(
        config['hdfs']['bronze_stage_dir'],
        datetime.today().strftime("%Y-%m-%d"),
        'out_of_stock',
        'data.csv')

    if not hdfs.is_file_exist(hdfs_file_path):
        logging.info('The out_of_stock is empty!')
        df = spark.create_empty_data_frame(['product_id', 'date'])
    else:
        df = spark.read_from_bronze(hdfs_file_path)

    func = F.udf(lambda x: datetime.strptime(x, '%Y-%m-%d'), DateType())

    df = df.withColumn("product_id", df["product_id"].cast(IntegerType())) \
        .withColumn('date', func(df['date']))

    spark.save_to_silver('out_of_stock', df)
    logging.info('The table "out_of_stock" uploaded to bronze!')

