# coding=utf-8
import json
import os

from hdfs import InsecureClient

class HdfsService:
    __conf = None
    __client = None

    def __init__(self, conf):
        self.__conf = conf
        self.__client = InsecureClient(
            self.__conf['hdfs']['url'],
            user=self.__conf['hdfs']['user'])

    def __create_directory_if_not_exist(self, file_path):
        if self.__client.content(os.path.dirname(file_path), strict=False) is None:
            self.__client.makedirs(os.path.dirname(file_path))

    def __rm_file_if_exist(self, file_path):
        if self.__client.content(file_path, strict=False) is not None:
            self.__client.delete(file_path)

    def upload_to_storage(self, file_path, local_file_path):
        self.__create_directory_if_not_exist(file_path)
        self.__rm_file_if_exist(file_path)

        self.__client.upload(file_path, local_file_path)

    def is_file_exist(self, file_path):
        if self.__client.content(file_path, strict=False) is None:
            return False
        return True
