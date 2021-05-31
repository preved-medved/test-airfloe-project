# coding=utf-8
import os
import csv
import json


class FileStorageService:
    __conf = None

    def __init__(self, conf):
        self.__conf = conf

    def create_directory_if_not_exist(self, file_path):
        if not os.path.exists(file_path):
            try:
                os.makedirs(file_path)
            except OSError:
                raise Exception("Can't create directory")

    def save_as_json(self, file_path, data):
        self.create_directory_if_not_exist(os.path.dirname(file_path))

        with open(file_path, 'w') as stream:
            stream.write(json.dumps(data))

    def save_as_csv(self, file_path, data):
        self.create_directory_if_not_exist(os.path.dirname(file_path))

        csv_columns = data[0].keys()
        with open(file_path, 'w') as stream:
            writer = csv.DictWriter(stream, fieldnames=csv_columns)
            writer.writeheader()
            for row in data:
                writer.writerow(row)


    def rm_file_if_exist(self, file_path):
        if os.path.exists(file_path):
            os.remove(file_path)