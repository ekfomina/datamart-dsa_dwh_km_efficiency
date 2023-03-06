"""
В данном модуле содержатся классы для сбора и отправки статистик в CTL
"""
import json
import logging
from datetime import datetime
from typing import List, Tuple

import requests

logger = logging.getLogger(__name__)


class Statistics:
    def __init__(self,
                 entity_id: int,
                 loading_id: int = None,
                 ctl_validfrom: str = None,
                 application_id: str = None,
                 business_date: str = None,
                 max_cdc_date: str = None,
                 change: int = 0,
                 relevance_value: str = "",
                 table_rows_number_arr: List[Tuple[int, str, int]] = None):
        # CTL
        self.entity_id = entity_id
        self.loading_id = loading_id
        self.ctl_validfrom = ctl_validfrom or datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.application_id = application_id
        self.business_date = business_date
        self.max_cdc_date = max_cdc_date
        self.change = change
        # KPD report
        self.relevance_value = relevance_value
        self._table_rows_number_arr = table_rows_number_arr or []

    def __min_attribute__(self, att_name: str, value):
        att_value = self.__dict__.get(att_name, None)
        if value is not None and (att_value is None or value < att_value):
            self.__dict__[att_name] = value

    def update_business_date(self, value):
        self.__min_attribute__("business_date", value)

    def update_max_cdc_date(self, value):
        self.__min_attribute__("max_cdc_date", value)

    def add_rows_number(self, step: int, table_name: str, rows_number: int):
        """
        Добавляет в массив названий таблиц с количество строчек новую запись,
        если название таблицы не 'None'
        Args:
            step - шаг, на котором выполнено обновление таблицы
            table_name - {схема таблицы}.{название таблицы} - полное название таблицы
            rows_number - Количество записей в таблице полсе нового рачёта
        """
        if table_name is not None:
            self._table_rows_number_arr.append((step, table_name, rows_number))

    @property
    def table_rows_number_arr(self):
        """
        Returns:
            [(Step, Full table name, Rows number)]
        """
        if self._table_rows_number_arr:
            return self._table_rows_number_arr
        else:
            return [(0, None, 0)]

    @property
    def computed_tables_set(self):
        if self._table_rows_number_arr:
            return set(list(zip(*self.table_rows_number_arr))[1])
        else:
            return set()


class CTLStatisticsServer:
    def __init__(self, ctl_url: str, loading_id: int, ctl_entity_id: int):
        """
        Args:
            ctl_url: url для обращений к CTL по rest
            loading_id: id конкретного запуска потока
            ctl_entity_id: id cущности куда статистики публиковать
        """
        self._ctl_url = ctl_url
        self._loading_id = loading_id
        self._ctl_entity_id = ctl_entity_id

    def upload_statistic(self, stat_id: int, stat_value: str):
        """
        Публикация статистики в CTL

        Args:
            stat_id: номер публикуемой статистики (см в CTL)
            stat_value: значение публикуемой статистики

        Returns: None
        """
        logger.debug("upload stat with stat_id: {}".format(stat_id))

        data = {"loading_id": int(self._loading_id),
                "entity_id": int(self._ctl_entity_id),
                "stat_id": int(stat_id),
                "avalue": [str(stat_value)]}

        header = {"Content-Type": "application/json"}

        response = requests.post(url=self._ctl_url,
                                 data=json.dumps(data),
                                 headers=header)

        logger.debug("response status code: {}".format(response.status_code))
        if not response.ok:
            logger.error("ctl statistic with id: {} not uploading".format(stat_id))

    def upload_statistics(self, statistics: Statistics):
        """
            Публикация всех стандартных статистик в CTL

            Args:
                statistics: структура, хранящая в себе основные статистики:
                             application id, ctl_validfrom, change, business date, max cdc date
        """
        self.upload_statistic(stat_id=15, stat_value=statistics.application_id)
        logger.info("{ctl_entity_id}: YARN application for id '{application_id}' was published"
                    .format(ctl_entity_id=self._ctl_entity_id,
                            application_id=statistics.application_id))

        self.upload_statistic(stat_id=11, stat_value=statistics.ctl_validfrom)
        logger.info("{ctl_entity_id}: LAST_LOADED_TIME '{ctl_validfrom}' was published"
                    .format(ctl_entity_id=self._ctl_entity_id,
                            ctl_validfrom=statistics.ctl_validfrom))

        if statistics.change:
            self.upload_statistic(stat_id=2, stat_value=str(statistics.change))
            logger.info("{ctl_entity_id}: CHANGE was published".format(ctl_entity_id=self._ctl_entity_id))
        else:
            logger.info("{ctl_entity_id}: NO CHANGE".format(ctl_entity_id=self._ctl_entity_id))

        self.upload_statistic(stat_id=5, stat_value=statistics.business_date)
        logger.info("{ctl_entity_id}: BUSINESS_DATE '{business_date}' was published"
                    .format(ctl_entity_id=self._ctl_entity_id,
                            business_date=statistics.business_date))

        self.upload_statistic(stat_id=1, stat_value=statistics.max_cdc_date)
        logger.info("{ctl_entity_id}: MAX_CDC_DATE '{max_cdc_date}' was published"
                    .format(ctl_entity_id=self._ctl_entity_id,
                            max_cdc_date=statistics.max_cdc_date))

        # Send CTL statistics
        logger.ctl_info("{ctl_entity_id}: "
                        "SUCCESS. YARN Application id: {application_id}. "
                        "CHANGE = {change}. New MAX_CDC_DATE = {max_cdc_date}. "
                        "New BUSINESS_DATE = {business_date}. "
                        .format(ctl_entity_id=self._ctl_entity_id,
                                application_id=statistics.application_id,
                                change=statistics.change,
                                max_cdc_date=statistics.max_cdc_date,
                                business_date=statistics.business_date))

    @staticmethod
    def get_server_upload_statistics(ctl_url: str, statistics: Statistics) -> 'CTLStatisticsServer':
        """
            Инициализация CTL сервера для публикации статистик сущности, привязанных к запущенному потоку,
            и отправка всех стандартных статистик на него.

            Args:
                ctl_url: базовый адрес CTL сервера
                statistics: структура, хранящая в себе данные о запуске и сущности и основные статистики:
                             loading id, entity id
                             application id, ctl_validfrom, change, business date, max cdc date

            Return:
                CTLStatisticsServer - сервер для публикации статистик сущности, привязанных к запущенному потоку
        """
        server = CTLStatisticsServer(ctl_url + '/v1/api/statval/m',
                                     statistics.loading_id,
                                     statistics.entity_id)
        server.upload_statistics(statistics)
        return server
