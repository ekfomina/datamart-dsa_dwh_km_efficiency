"""
В данный модуль вынесен функционал, связанный с управлением файлами в распрелённой файловой системе (hdfs)
"""
import logging
import os
import re
from typing import List
from pyspark import SparkContext
from utils.etl import EtlExecutor

from datetime import datetime
from dateutil import relativedelta
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)


class HadoopFileSystem:
    def __init__(self, spark_context: SparkContext):
        hadoopConfiguration = spark_context._jsc.hadoopConfiguration()
        # Access to java hadoop libs
        self._fs = spark_context._jvm.org.apache.hadoop.fs
        self._file_system = self._fs.FileSystem.get(hadoopConfiguration)
        self._block_size = hadoopConfiguration.getInt("dfs.block.size", 100 * 1024 * 1024)
        logger.debug("Block size is {0} bytes".format(self._block_size))

    @property
    def block_size_in_bytes(self) -> int:
        return self._block_size

    def move(self, origin_path: str, target_path: str):
        self._file_system.rename(self._fs.Path(origin_path), self._fs.Path(target_path))

    def create_directory(self, path_to_dir: str, permission: str = ""):
        if self._file_system.exists(self._fs.Path(path_to_dir)):
            logger.debug("This directory already exists: {}".format(path_to_dir))
            return
        if permission == "":
            self._file_system.mkdirs(self._fs.Path(path_to_dir))
        else:
            self._file_system.mkdirs(self._fs.Path(path_to_dir),
                                     self._fs.permission.FsPermission(permission))
        logger.debug("Create {}".format(path_to_dir))

    def clear_directory(self, path_to_dir: str, keep_directory=False):
        if not self._file_system.exists(self._fs.Path(path_to_dir)):
            logger.debug("No such directory {}".format(path_to_dir))
            return
        logger.debug("Delete all files in {}".format(path_to_dir))
        if not self._file_system.delete(self._fs.Path(path_to_dir), True):
            logger.warning("Deleting wasn't completed")
        if keep_directory:
            self._file_system.mkdirs(self._fs.Path(path_to_dir))
        else:
            logger.debug("Including the directory itself")

    def get_file_size_in_bytes(self, path_to_file) -> int:
        return self._file_system.getContentSummary(self._fs.Path(path_to_file)).getLength()

    def get_directories(self, source_dir: str, pattern: str = ".*", recursive: bool = False) -> List[str]:
        """
        Возвращает список вложенных неслужебных директорий

        Args:
            source_dir: путь до исследуемой директории
            pattern: регулярное выражение, фильтрующее названия директорий
            recursive: флаг, отвечающий за перечисление директорий дальше первого уровня вложенности
        """
        # Remove scheme and authority from path
        explored_dir = self._fs.Path.getPathWithoutSchemeAndAuthority(self._fs.Path(source_dir))
        explored_path = str(explored_dir)
        # Prepare metadata for checking temporary names
        temp_name_beginnings_pattern = "/[_\.]"
        guaranteed_part_of_name = len(explored_path) - 1 if explored_path[-1] == "/" else len(explored_path)
        dir_list = []
        for file in self._file_system.listStatus(explored_dir):
            path = str(self._fs.Path.getPathWithoutSchemeAndAuthority(file.getPath()))
            # Filter directories which is not temporary
            if file.isDirectory() \
                    and re.search(temp_name_beginnings_pattern, path[guaranteed_part_of_name:]) is None \
                    and re.fullmatch(pattern, path) is not None:
                if recursive:
                    nested_dirs = self.get_directories(path, pattern, True)
                    if nested_dirs:
                        dir_list.extend(nested_dirs)
                        continue
                dir_list.append(path)

        logger.debug("The following directories was found:\n" + ",\n".join(dir_list))
        return dir_list

    def get_directories_with_data(self, source_dir: str, pattern: str = ".*", recursive: bool = False) -> List[str]:
        """
            Возвращает список вложенных неслужебных директорий c данными

            Args:
                source_dir: путь до исследуемой директории
                pattern: регулярное выражение, фильтрующее названия директорий
                recursive: флаг, отвечающий за перечисление директорий дальше первого уровня вложенности
        """

        def check_data_and_pattern(path_to_dir: str):
            data_size = self.get_file_size_in_bytes(path_to_dir)
            return data_size > 0

        appropriate_dirs = list(filter(check_data_and_pattern, self.get_directories(source_dir, pattern, recursive)))
        logger.debug("The following directories was left:\n" + ",\n".join(appropriate_dirs))
        return appropriate_dirs

    def get_modification_time(self, path_to_file: str) -> datetime.strptime:
        """
            Определяет дату последнего изменения файла/директории
            Args:
                path_to_file: путь до файла/директории
            Returns:
                Дата изменения объекта
        """
        location = self._fs.Path(path_to_file)
        modification_timestamp = self._file_system.getFileStatus(location).getModificationTime()
        modification_date = datetime.fromtimestamp(modification_timestamp / 1000).strftime('%Y-%m-%d %H:%M:%S')
        return modification_date


class PartitionController:
    def __init__(self, etl_executor: EtlExecutor, file_system: HadoopFileSystem,
                 number_blocks_in_file: float = 5):
        """
            Args:
                etl_executor: интерфейс для исполнения etl запросов
                file_system: интерфейс для взаимодействия с файловой системой
                number_blocks_in_file: целевое примерное количество блоков,
                                                которое занимает один файл (партиция) в hdfs
        """
        self._etl_executor = etl_executor
        self._file_system = file_system
        self.number_blocks_in_file = number_blocks_in_file

    @staticmethod
    def _get_partition_value(path: str, partition_column: str = '') -> str:
        value_match = re.search("/{partition_column}=([^/]*)".format(partition_column=partition_column), path)
        if value_match is not None:
            return value_match.group(1)

        if partition_column != "":
            logger.warning("Partition value of {partition} was not found for path\n{path}"
                           .format(partition=partition_column, path=path))
        return ''

    def _get_date_filter_directory(self, partition_column: str = '', min_value: str = '', max_value: str = ''):
        """
        Генератор фильтров для партиций таблицы
        Args:
            partition_column: название колонки таблицы, по которой происходит фильтрация;
                              если аргумент пустой, фильтрация осуществляется по времени изменения партиций
            min_value: дата или другое значение, начиная с которого включительно принимаются партиции;
                       по умолчанию нижняя граница отсутствует
            max_value: дата или другое значение, до которого включительно принимаются партиции;
                       по умолчанию верхняя граница отсутствует
        Returns:
            Фильтр имён/адресов партиций таблицы по дате
        """

        def date_filter_directory(path: str):
            # Date filter
            if partition_column:
                value = PartitionController._get_partition_value(path, partition_column)
            else:
                value = self._file_system.get_modification_time(path)
            logger.debug("Filtration value is {}".format(value))
            suit = (min_value <= value)
            if max_value != '':
                suit = suit and (value <= max_value)

            return suit

        return date_filter_directory

    def _get_repart_factor(self, source_dir: str) -> int:
        """
            Рассчитвает примерное количество файлов, на которые необходимо разбить данные в переданной директории,
            с учётом их заданного целевого размера.

            Args:
                source_dir: путь до директории с parquet файлами
        """
        size_in_bytes = self._file_system.get_file_size_in_bytes(source_dir)
        if size_in_bytes == 0:
            logger.info("Current directory is empty")
            return 0

        logger.debug("Size in bytes " + str(size_in_bytes))
        repart_factor = int(size_in_bytes //
                            (self._file_system.block_size_in_bytes * self.number_blocks_in_file)) + 1
        logger.info("Repartition = " + str(repart_factor))
        return repart_factor

    def delete_partitions(self, table_name: str, partition_column: str = '', from_date: str = '', to_date: str = ''):
        """
        Удаляет партиции таблицы, отвечающие за указанный промежуток времени.

        Args:
            table_name: полное название репартицируемой таблицы вместе со схемой
            partition_column: название колонки таблицы, по которой происходит фильтрация;
                              по умолчанию фильтрация осуществляется по времени изменения партиций
            from_date: дата или другое значение, начиная с которого включительно удаляются партиции;
                       по умолчанию нижняя граница отсутствует
            to_date: дата или другое значение, до которого включительно удаляются партиции;
                     по умолчанию верхняя граница отсутствует
        """

        logger.info("Delete outdated partitions in " + table_name)
        table_location_dir = self._etl_executor.get_hdfs_location(table_name)
        dir_list = self._file_system.get_directories(table_location_dir, recursive=True)
        if not dir_list:
            # Для НЕпартиционированной таблицы
            self._file_system.clear_directory(table_location_dir, keep_directory=True)

        else:
            # Для партицинированной таблицы
            logger.debug("Delete from date {}".format(from_date))
            logger.debug("Delete to date {}".format(to_date))
            dir_list = list(filter(self._get_date_filter_directory(partition_column, from_date, to_date), dir_list))
            logger.debug("File_list: {}".format(str(dir_list)))
            for file in dir_list:
                self._file_system.clear_directory(file)

    def delete_outdated_partitions(self, table_name: str, lifetime_in_days: int,
                                   partition_column: str = '', curr_date: str = ''):
        """
        Удаляет устаревшие партиции таблицы.

        Args:
            table_name: полное название репартицируемой таблицы вместе со схемой
            lifetime_in_days: число дней актуальности партиции;
                              если значение равно нулю, удаление не производится
            partition_column: название колонки таблицы, по которой происходит фильтрация;
                              по умолчанию фильтрация осуществляется по времени последнего изменения партиций
            curr_date: дата, от которой происходит отсчёт актуальности данных, в формате '%Y-%m-%d %H:%M:%S';
                       текущая дата по умолчанию
        """
        if not lifetime_in_days:
            return
        date_format = '%Y-%m-%d %H:%M:%S'
        curr_date = datetime.strptime(curr_date, date_format) if curr_date else datetime.now()
        last_date = curr_date - relativedelta.relativedelta(days=lifetime_in_days)
        last_date = last_date.strftime(date_format)
        self.delete_partitions(table_name, partition_column, to_date=last_date)

    def repart(self, table_name: str, partition_column: str = '', start_date: str = '', last_date: str = ''):
        """
        Укрупняет parquet файлы в партициях, находящихся в переданной директории, или в самой директории,
        если она состоит из них.
        Все вложенные директории (только по переданному пути) рассматриваются как партции по дате и фильтруются по
        заданному минимальному значению.

        Args:
            table_name: полное название репартицируемой таблицы вместе со схемой
            partition_column: название колонки таблицы, по которой происходит фильтрация;
                              по умолчанию фильтрация осуществляется по времени последнего изменения партиций
            start_date: дата или другое значение, начиная с которого включительно укрупняются партиции;
                        по умолчанию нижняя граница отсутствует
            last_date: дата или другое значение, до которого включительно укрупняются партиции;
                       по умолчанию верхняя граница отсутствует
        """

        table_location_dir = self._etl_executor.get_hdfs_location(table_name)
        dir_list = self._file_system.get_directories(table_location_dir, recursive=True)
        if len(dir_list) == 0:
            # Для НЕпартиционированной таблицы
            self.repart_non_partitioned_table(table_name, table_location_dir)

        else:
            # Для партицинированной таблицы
            logger.debug("Partitions from start date {} to last date {}".format(start_date, last_date))
            dir_list = list(filter(self._get_date_filter_directory(partition_column, start_date, last_date),
                                   dir_list))
            logger.debug("File_list: {}".format(str(dir_list)))

            if dir_list:
                with ThreadPoolExecutor(len(dir_list)) as executor:
                    for directory in dir_list:
                        executor.submit(self.repart_one_partition, table_name, table_location_dir, directory)
                        logger.debug("Directory {}".format(directory))

    def repart_non_partitioned_table(self, table_name: str, table_location_dir: str):
        """
        Укрупняет паркетники, находящиеся в переданной директории непартиционированной таблицы.

        Args:
            table_name: полное название репартицируемой таблицы вместе со схемой
            table_location_dir: путь до корневой директории, в которой хранятся данные таблицы
        """

        logger.info("Repart files in the non-partitioned table " + table_name)
        repart_factor = self._get_repart_factor(table_location_dir)
        if repart_factor == 0:
            return None

        # Move original files to temporary directory
        tmp_dir_for_computation = table_location_dir + "_tmp_for_computation"
        self._file_system.clear_directory(tmp_dir_for_computation)
        logger.debug("Move " + table_location_dir + " --> " + tmp_dir_for_computation)
        self._file_system.move(table_location_dir, tmp_dir_for_computation)

        logger.debug("Start repartition")
        self._file_system.create_directory(table_location_dir)
        self._etl_executor.insert_data_from_dir(tmp_dir_for_computation, table_name,
                                                repartition_factor=repart_factor,
                                                overwrite=True)

        # Delete temporal directory
        self._file_system.clear_directory(tmp_dir_for_computation)

    def repart_one_partition(self, table_name: str, table_location_dir: str, partition_dir: str):
        """
        Укрупняет паркетники, находящиеся в переданной директории (не рассматривая вложенные директории).

        Args:
            table_name: полное название репартицируемой таблицы вместе со схемой
            table_location_dir: путь до корневой директории, в которой хранятся данные таблицы
            partition_dir: путь до директории определённой партиции с parquet файлами
        """

        logger.info("Repart files of the table {table_name} in partition {partition}".
                    format(table_name=table_name,
                           partition=os.path.basename(partition_dir)))
        repart_factor = self._get_repart_factor(partition_dir)
        if repart_factor == 0:
            return None

        logger.debug("Start repartition")
        self._etl_executor.insert_data_from_dir(partition_dir, table_name,
                                                base_source_dir=table_location_dir,
                                                repartition_factor=repart_factor,
                                                overwrite=True)
