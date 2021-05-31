# coding=utf-8
import logging
import requests


class RdApiService:
    __conf = None
    __token = None

    def __init__(self, conf):
        self.__conf = conf
        self.__token = self.__get_token()

    def __get_token(self):
        url = 'https://%s/auth' % self.__conf['out_of_stock']['url']
        response = requests.post(url, json=self.__get_credentials(), timeout=30)

        if response.status_code != 200:
            raise Exception("Auth Request fail")

        try:
            return response.json()['access_token']
        except Exception:
            raise ValueError("Can't get 'access_token'")

    def __get_credentials(self):
        return {
            "username": self.__conf['out_of_stock']['user'],
            "password": self.__conf['out_of_stock']['password']
        }

    def get_out_of_stock(self, for_date):
        url = 'https://%s/out_of_stock' % self.__conf['out_of_stock']['url']
        headers = {
            'Authorization': 'JWT %s' % self.__token,
        }
        response = requests.get(url, headers=headers, params={"date": for_date}, timeout=30)

        if response.status_code not in [200, 404]:
            raise Exception("Request 'out_of_stock' is failed")

        try:
            content = response.json()
            if isinstance(content, dict) \
                    and 'message' in content.keys() \
                    and content['message'] == 'No out_of_stock items for this date':
                return []
            return content
        except Exception:
            raise ValueError("Can't get data from request 'out_of_stock'")
