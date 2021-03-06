# coding=utf-8
import json
import logging
from airflow.hooks.base_hook import BaseHook


def get_config():
    oltp_postgres = BaseHook.get_connection("oltp_postgres")
    oltp_postgres_extra = get_from_extra(
        oltp_postgres.extra,
        ['database'])

    oltp_greenplum = BaseHook.get_connection("oltp_greenplum")
    oltp_greenplum_extra = get_from_extra(
        oltp_greenplum.extra,
        ['database'])

    hdfs_connection = BaseHook.get_connection("hdfs_connection")
    hdfs_connection_extra = get_from_extra(
        hdfs_connection.extra,
        ['bronze_stage_dir']
    )

    out_of_stock_api = BaseHook.get_connection("out_of_stock_api")
    odbc_driver_path = BaseHook.get_connection("odbc_driver_path")

    return {
        'pg_creds': {
            'host': oltp_postgres.host,
            'port': oltp_postgres.port,
            'database': oltp_postgres_extra['database'],
            'user': oltp_postgres.login,
            'password': oltp_postgres.password
        },
        'greenplum_creds': {
            'url': 'jdbc:postgresql://%s:%s/%s' % (
                oltp_greenplum.host,
                oltp_greenplum.port,
                oltp_greenplum_extra['database']),
            'user': oltp_greenplum.login,
            'password': oltp_greenplum.password
        },
        'hdfs': {
            'url': 'http://%s:%s/' % (hdfs_connection.host, hdfs_connection.port),
            'user': hdfs_connection.login,
            'bronze_stage_dir': hdfs_connection_extra['bronze_stage_dir']
        },
        'out_of_stock': {
            'url': out_of_stock_api.host,
            'user': out_of_stock_api.login,
            'password': out_of_stock_api.password
        },
        'odbs_driver_path': odbc_driver_path.host
    }


def get_from_extra(extra=None, fields=[]):
    extra = json.loads(extra)
    for field in fields:
        if field not in extra.keys():
            raise Exception("Config extra have no variable '%s'" % field)
    return extra
