"""
В данный модуль вынесен функционал, связанный с обработкой аргументов, переданных скрипту
"""
import json
import re

from argparse import ArgumentParser
from collections import defaultdict


def json_dict(input_data: str) -> dict:
    """
    Args:
        input_data: входная строка формата json с одинарными кавычками

    Returns:
        Python словарь
    """
    return json.loads(input_data.replace("'", '"'))


class KeyDefaultDict(defaultdict):
    def __missing__(self, key):
        if self.default_factory is None:
            return None
        else:
            value = self[key] = self.default_factory(key)
            return value


def get_default_json_dict(default_value=None):
    """
        Args:
            default_value: (key) -> value

        Returns:
            Формат данных словаря с значениями по умолчанию для парсера
    """
    def default_json_dict(input_data: str) -> KeyDefaultDict:
        """
            Args:
                input_data: входная строка формата json с одинарными кавычками

            Returns:
                Python словарь с значениями по умолчанию
        """
        values = json_dict(input_data)
        return KeyDefaultDict(default_value, values)

    return default_json_dict


def default_path(full_table_name: str) -> str:
    """
    Возвращает путь по умолчанию, по которому таблица физически лежит в hdfs
        Args:
            full_table_name: {схема таблицы}.{название таблицы} - полное название таблицы

        Returns:
            Путь по умолчанию, по которому располагаются данные таблицы в HDFS
    """
    stg_pattern = "custom_salesntwrk_(.*)_stg\.(.*)"
    pa_pattern = "custom_salesntwrk_(.*)\.(.*)" # Более общий паттерн
    if re.fullmatch(stg_pattern, full_table_name) is not None:
        schema, table = re.fullmatch(stg_pattern, full_table_name).groups()
        path = '/data/custom/salesntwrk/{}/stg/{}'.format(schema, table)
    elif re.fullmatch(pa_pattern, full_table_name) is not None:
        schema, table = re.fullmatch(pa_pattern, full_table_name).groups()
        path = '/data/custom/salesntwrk/{}/pa/{}'.format(schema, table)
    else:
        path = ''

    return path


def parse_arguments():
    parser = ArgumentParser()
    parser.add_argument("--etl_trgt_tbls",
                        dest="etl_trgt_tbls",
                        type=get_default_json_dict(),
                        help="dictionary of names of computed in certain steps target tables",
                        default="{}")
    parser.add_argument("--etl_stg_tbls",
                        dest="etl_stg_tbls",
                        type=get_default_json_dict(),
                        help="dictionary of names of computed in certain steps staged tables",
                        default="{}")
    parser.add_argument("--etl_tbls_loc",
                        dest="etl_tbls_loc",
                        type=get_default_json_dict(default_path),
                        help="dictionary of hdfs paths for data tables",
                        default="{}")
    parser.add_argument("--business_date_columns",
                        dest="business_date_columns",
                        type=get_default_json_dict(),
                        help="name of the columns that is responsible for data relevance in target tables",
                        default="{}")
    parser.add_argument("--trgt_lifetime_in_days",
                        dest="trgt_lifetime_in_days",
                        type=get_default_json_dict(lambda x: 730),
                        help="dictionary of data lifetimes for target tables",
                        default="{}")
    parser.add_argument("--parquet_size_in_blocks",
                        dest="parquet_size_in_blocks",
                        type=float,
                        help="target approximate size of one parquet file in blocks",
                        default=1.85)
    parser.add_argument("--etl_src_tbls",
                        dest="etl_src_tbls",
                        type=json_dict,
                        help="full names of source tables",
                        default="{}")
    parser.add_argument("--relevance_value",
                        dest="relevance_value",
                        type=str,
                        help="custom value of computed data relevance",
                        default="-")
    parser.add_argument("--etl_force_load",
                        dest="etl_force_load",
                        type=int,
                        help="flag of archive loading",
                        default=0)
    parser.add_argument("--recovery_mode",
                        dest="recovery_mode",
                        type=int,
                        help="flag of recovery loading with using computed stage table",
                        default=0)

    # Replaced by environment vars
    parser.add_argument("--loading_id",
                        dest="loading_id",
                        type=int,
                        help="loading_id from CTL",
                        required=False)
    parser.add_argument("--ctl_url",
                        dest="ctl_url",
                        type=str,
                        help="ctl url for use rest api",
                        required=False)
    parser.add_argument("--ctl_entity_id",
                        dest="ctl_entity_id",
                        type=int,
                        help="id ctl workflow",
                        required=False)

    known_args, unknown_args = parser.parse_known_args()

    return known_args
