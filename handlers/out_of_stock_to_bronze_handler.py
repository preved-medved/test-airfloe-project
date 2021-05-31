# coding=utf-8
import os
import logging
from datetime import date
from services.hdfs_service import HdfsService
from services.config_service import get_config
from services.rd_api_service import RdApiService
from services.file_storage_service import FileStorageService


def out_of_stoke_to_bronze():
    config = get_config()

    curr_date = date.today().strftime("%Y-%m-%d")

    rd_source = RdApiService(config)
    file_storage = FileStorageService(config)
    hdfs = HdfsService(config)
    storage = FileStorageService(config)

    local_file_path = '/tmp/out_of_stock-%s.csv' % curr_date
    hdfs_file_path = os.path.join(
        config['hdfs']['bronze_stage_dir'],
        curr_date,
        'out_of_stock',
        'data.csv')

    storage.rm_file_if_exist(local_file_path)

    out_of_stock = rd_source.get_out_of_stock(curr_date)
    if len(out_of_stock) == 0:
        logging.info('out_of_stock is empty for today')
        return

    file_storage.save_as_csv(local_file_path, out_of_stock)
    hdfs.upload_to_storage(hdfs_file_path, local_file_path)

    storage.rm_file_if_exist(local_file_path)

    logging.info('Out of stock was successfully loaded')
