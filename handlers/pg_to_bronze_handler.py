# coding=utf-8
import os
import logging
from datetime import date

from services.psql_service import PsqlService
from services.hdfs_service import HdfsService
from services.config_service import get_config
from services.file_storage_service import FileStorageService


def pg_to_bronze(table_name=None):
    config = get_config()

    psql = PsqlService(config)
    hdfs = HdfsService(config)
    storage = FileStorageService(config)

    local_file_path = os.path.join('/tmp', '%s.csv' % table_name)
    hdfs_file_path = os.path.join(
        config['hdfs']['bronze_stage_dir'],
        date.today().strftime("%Y-%m-%d"),
        config['pg_creds']['database'],
        '%s.csv' % table_name)

    storage.rm_file_if_exist(local_file_path)

    psql.dump_to_csv_file(local_file_path, table_name)
    hdfs.upload_to_storage(hdfs_file_path, local_file_path)

    storage.rm_file_if_exist(local_file_path)

    logging.info('Table "%s" was successfully loaded' % table_name)
