"""
В данный модуль вынесен функционал, связанный с выполнением ETL запросов (в Spark).
"""
import glob
import logging
import re
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from typing import List, Tuple, Dict

logger = logging.getLogger(__name__)


class EtlExecutor:
    def __init__(self, app_name: str):
        # Получение спарк сессии
        self._spark = (SparkSession.builder
                       .appName(app_name)
                       .enableHiveSupport()
                       .getOrCreate())

        blockSize = 1024 * 1024 * 128
        sc = self._spark.sparkContext
        sc._jsc.hadoopConfiguration().setInt("df.blocksize", blockSize)
        sc._jsc.hadoopConfiguration().setInt("parquet.blocksize", blockSize)
        sc._jsc.hadoopConfiguration().setInt("dfs.blocksize", blockSize)
        sc._jsc.hadoopConfiguration().setInt("dfs.block.size", blockSize)

    @property
    def spark(self) -> SparkSession:
        """
        Возвращает Spark сессию, в которой выполняются все запросы.
        Не рекомендумеется ичпользовать через неё PySpark API на прямую,
        поле преднозначено для извлечения контекстной информации.
        (Для ETL запросов используйте текущий класс).
        """
        return self._spark

    @staticmethod
    def _get_sql_queries(glob_file_pattern: str) -> List[str]:
        """
        Читает tasks-ные запросы из файлов, подходящие под glob паттерн.

        Args:
            glob_file_pattern: glob шаблон для имён файлов с tasks запросами

        Returns:
            tasks-ный запрос

        """
        query_files = glob.glob(glob_file_pattern)
        if len(query_files) == 0:
            logger.error("There are not files on path: " + glob_file_pattern)

        queries = []
        for file_name in query_files:
            with open(file_name) as f:
                query = f.read()
            queries.append(query)

        return queries

    def execute_sql_queries(self, queries: List[str], **kwargs):
        """
        Исполняет переданный список запросов, предварительно разибивая их на отдельные SQL команды.

        Args:
            queries: список SQL запросов
            kwargs: параметры для форматирования запроса

        """
        for query in queries:
            f_query = query.format(**kwargs)
            for sub_query in f_query.split(";"):
                logger.debug("Executing query:\n" + sub_query)
                self._spark.sql(sub_query)

    def execute_sql_queries_from(self, glob_pattern_path: str, **kwargs):
        """
        Исполняет запросы из файлов, подходящих под glob выражения.

        Args:
            glob_pattern_path: glob шаблон для имён файлов с tasks запросами
            kwargs: параметры для форматирования запроса

        """
        queries = self._get_sql_queries(glob_pattern_path)
        self.execute_sql_queries(queries, **kwargs)

    def get_stat_value(self, query: str):
        """
        Получает статистические данные (благодоря тому, что исполняет переданный запрос).

        Args:
            query: tasks-ный запрос

        Returns:
            Значение, которое выдал spark по запросу
        """
        logger.debug("Executing query:\n" + query)
        df = self._spark.sql(query)
        stat_value = str(df.collect()[0][0])
        logger.debug("Result of query: " + stat_value)
        return stat_value

    def get_stat_value_from(self, glob_pattern_path: str, **kwargs):
        """
        Получает статистические данные по запросу из первого файла, подходящего под glob выражение
        (благодоря тому, что исполняет переданный запрос).

        Args:
            glob_pattern_path: glob шаблон для имени файла с tasks запросом
            kwargs: параметры для форматирования запроса

        Returns:
            Значение, которое выдал spark по форматированному запросу

        """
        query = self._get_sql_queries(glob_pattern_path)[0].format(**kwargs)
        stat_value = self.get_stat_value(query)
        return stat_value

    def get_hdfs_location(self, table_name: str) -> str:
        """
        Возвращает путь, по которому таблица физически лежит в hdfs.

        Args:
            table_name: {схема таблицы}.{название таблицы} - полное название таблицы

        Returns:
            Путь, по которому таблица физически лежит в hdfs
            Пример:
            'hdfs://sna-dev/data/custom/salesntwrk/models_dm_sverka/pa/txn_fraud'
        """
        query = "DESC FORMATTED {table_name}".format(table_name=table_name)

        df = self._spark.sql(query)
        df = df.filter("col_name=='Location'")
        hdfs_location = df.collect()[0].data_type

        return hdfs_location

    def insert_data_from_dir(self, source_dir: str, to_table_name: str,
                             repartition_factor: int, overwrite: bool, base_source_dir: str = None):
        """
        Загружает данные из директории hdfs в таблицу.

        Args:
            source_dir: путь до директории в hdfs
            to_table_name: {схема таблицы}.{название таблицы} - полное название таблицы
            repartition_factor: количество parquet файлов, на которые будут разбиты
                загруженные данные в целевой таблице
            overwrite: флаг перезаписи партиции или всей таблицы, если она не репартиционированная
            base_source_dir: опфиональный путь до базовой директории hdfs с загружаемыми данными
                для сохранения структуры партиционированных данных
        """
        logger.debug("Start reading from source")
        if base_source_dir is None:
            df = self._spark.read.parquet(source_dir)
        else:
            df = self._spark.read.option("basePath", base_source_dir).parquet(source_dir)
        logger.debug("Start insertion")
        df.repartition(repartition_factor).write.format("parquet").insertInto(to_table_name, overwrite=overwrite)

    def get_table_scheme(self, table_name: str) -> List[Tuple[str, str]]:
        """
        Возвращает список пар полей таблицы и их типов в правильном порядке.

        Args:
            table_name: полное название таблицы вместе со схемой
        """
        return self._spark.table(table_name).dtypes

    def get_parquet_scheme(self, path_to_storage: str) -> List[Tuple[str, str]]:
        """
        Возвращает список пар полей таблицы, хранящейся в файле, и их типов в правильном порядке.

        Args:
            path_to_storage: путь до parquet файла/директории с parquet файлами
        """
        return self._spark.read.parquet(path_to_storage).dtypes

    def cnt_rows(self, table_name: str, condition: str = "1=1") -> int:
        """
        Находит число строк в указаной таблице.

        Args:
            table_name: {схема таблицы}.{название таблицы} - полное название таблицы
                        если название пустое, возвращает 0
            condition: фильтр датафрейма по условию (опцонально)
        Returns:
            Число строк указанного датаферйма
        """
        if not table_name:
            return 0
        df = self._spark.table(table_name)
        num_rows = df.filter(condition).count()
        logger.debug("Number of records in {}: {}".format(table_name, num_rows))
        return num_rows

    def get_max_value(self, table_name: str, col_name: str, condition: str = ""):
        """
        Находит максимальное значение указанного элемента с применением фильтрации (если она есть).

        Args:
            table_name: {схема таблицы}.{название таблицы} - полное название таблицы
            col_name: название столбца, по которому производится вычисление
            condition: фильтр датафрейма по условию (опцонально)
        Returns:
            Максимальное значение элемента
        """
        logger.info("Get max value of {} in table {}".format(col_name, table_name))
        df = self._spark.table(table_name)
        try:
            res = df.filter(condition or "1=1") \
                .agg({col_name: "max"}) \
                .first()[0]
            logger.debug("Max value is {}".format(res))
        except TypeError as e:
            if df.count == 0:
                logger.warning("There are NO data in table = {} with condition {}".format(table_name, condition))
                return None
            else:
                raise TypeError("Cannot calculate max value from table {}. \n args = {}".format(table_name, e.args))
        return res

    def insert_single_row_data(self, table_name: str, **kwargs):
        """
        Добавляет переданные данные в таблицу.
        Берёт схему из указанной таблицы и на основании данной схемы формирует собственный датаферйм и загружет его.

        Args:
            table_name: {схема таблицы}.{название таблицы} - полное название таблицы
            **kwargs: данные, которые нужно загрузить
        Requires:
            Название переменных должно полностью совпадать с названием столбцов!
        """
        logger.debug("Read schema")
        df = self.spark.table(table_name)
        schema = df.schema

        value = {}
        for row in schema:
            value[row.name] = kwargs.get(row.name)

        data = [value]
        logger.debug("Create df with value = {}".format(value))
        insert_df = self.spark.createDataFrame(data, schema)

        logger.debug("Start to append data in table")
        insert_df.write.insertInto(table_name, overwrite=False)
        logger.info("Add row {} in {}".format(str(value), table_name))

    def _get_partitions_df(self, table_name: str):
        """
        Возвращает отсортированный по убыванию датафрейм со всеми значениями партиций.\n
        Пример:\n
        +------------------------------------------------+
        |partition                                       |
        +------------------------------------------------+
        |platformcolumn=MOBILE/timestampcolumn=2022-10-05|
        |platformcolumn=MOBILE/timestampcolumn=2022-10-04|
        +------------------------------------------------+

        Args:
            table_name: {схема таблицы}.{название таблицы} - полное название таблицы
        Returns:
            Датафрейм со значениями партиций
        """
        query = "SHOW PARTITIONS {}".format(table_name)
        df = self._spark.sql(query)
        return df

    def get_last_partition(self, table_name: str, specific_col: str = '') -> Dict[str, str]:
        """
        Возвращает значения колонок партиционирования в последней актуальной партиции.\n
        (Работает быстрее обычного поиска максимального значения).

        Args:
            table_name: {схема таблицы}.{название таблицы} - полное название таблицы
            specific_col: конкретное название поля партиционирования. [опционально]

        :returns:
            Последнюю партицию в формате словаря {'название колонки': 'значение', ...}.
            В случае передачи названия интересуемой колонки возвращается словарь из одного значения
        """
        partitions_df = self._get_partitions_df(table_name)
        partitions_df = partitions_df.orderBy(f.col("partition").desc()).first()
        last_partition = [] if partitions_df is None else partitions_df.partition.split('/')
        partition_dict = {}
        for column in last_partition:
            name, value = column.split('=')
            if not specific_col or name == specific_col:
                partition_dict[name] = value

        return partition_dict

    def recreate_non_partitioned_table(self,
                                       table_name: str,
                                       columns: List[Tuple[str, str]],
                                       location: str):
        """
        Пересоздаёт непартиционированную таблицу с указаннными параметрами.

        Args:
            table_name: {схема таблицы}.{название таблицы}
            columns: упорядоченный список пар полей таблицы и их типов [(имя, тип), ...]
            location: адрес хранения таблицы в hdfs
        """
        logger.info("Recreate table " + table_name)
        columns_string = ',\n'.join(map('\t'.join, columns))
        query = 'DROP TABLE IF EXISTS {table_name};\n' \
                'CREATE EXTERNAL TABLE IF NOT EXISTS {table_name} (\n' \
                '{columns_with_types}\n' \
                ')\n' \
                'STORED AS PARQUET\n' \
                'LOCATION "{location}"\n'

        self.execute_sql_queries([query],
                                 table_name=table_name,
                                 columns_with_types=columns_string,
                                 location=location)

    def transfer_table(self, source_table: str, target_table: str, columns: List[Tuple[str, str]], **kwargs):
        """
        Переносит данные из одной таблицы в другую.

        Args:
            source_table: {схема таблицы}.{название таблицы} для таблицы источника
            target_table: {схема таблицы}.{название таблицы} для целевой таблицы
            columns: упорядоченный список пар названий передаваемых в целевую таблицу полей и их типов;
                    названия полей должны совпадать с названиями колонок таблицы источника или содержаться в kwargs
                    [(имя, тип), ...]
            kwargs: предопределённые константные значения полей
        """
        logger.info("Transfer data from " + source_table + " to " + target_table)
        # Get original values
        column_names = list(zip(*columns))[0]
        source_df = self._spark.table(source_table).select(*column_names)
        # Commit value conversions
        main_values = []
        for i, (c_name, c_type) in enumerate(columns):
            is_custom_value = c_name in kwargs
            column = f.lit(kwargs[c_name]) if is_custom_value else source_df[c_name]
            if is_custom_value or c_type != source_df.dtypes[i]:
                column = column.cast(c_type)
            if c_name == "report_dt":
                column = column.substr(1, 10)
            main_values.append(column)
        # Insert prepared data to target tables
        target_df = source_df.select(*main_values)
        logger.debug("The following columns will be inserted into the target table:\n" +
                     ',\n'.join(map(': '.join, target_df.dtypes)))
        target_df.write.format("parquet").insertInto(target_table, overwrite=True)


class TaskWithStep:
    """
    Задача расчёта, состоящая из файла скрипта и порядкового номера шага (step).
    Запуск задач проиходит в возрастающем порядке шага.

    Например:
    1) step = 1
    2) step = 2
    3) step = 13

    Значение step должно принадлежать [0; +infinity).
    """

    def __init__(self, path_to_file: str, step: int, etl_executor: EtlExecutor):
        """
        Args:
            path_to_file: путь до файла, содержащего скрипт
            step: шаг исполнения
            etl_executor: исполнитель скрипта
        """

        assert isinstance(step, int)

        if step < 0:
            raise ValueError("step should be [0; +infinity)")

        self.path_to_file = path_to_file
        self.step = step
        self.etl_executor = etl_executor

    def __lt__(self, other):
        if self.step < other.step:
            return True
        else:
            return False

    def __call__(self, target_table_name: str = None, **kwargs) -> Tuple[int, int]:
        """
        Выполнение скрипта, содержащегося в файле задачи, указанным исполнителем.

        :arg target_table_name: {схема таблицы}.{название таблицы} -
                полное название целевой таблицы, которая параметризует {target_table_name} в запросе,
                и для которой будет подсчитываться изменение количества записей.
                Если отствует, количество записей будет заменено нулевым значением.
        :arg kwargs: дополнительные параметры для запуска скрипта задачи.

        :returns: Количество записей в целевой таблице до и после запроса.
        """
        logger.info('Start execute: step ' + str(self.step))
        logger.info("Calculating old count of records")
        previous_count_of_records = \
            self.etl_executor.cnt_rows(target_table_name) if target_table_name is not None else 0

        self._execute(target_table_name=target_table_name, **kwargs)

        logger.info("Calculating new count of records")
        current_count_of_records = \
            self.etl_executor.cnt_rows(target_table_name) if target_table_name is not None else 0
        return previous_count_of_records, current_count_of_records

    def _execute(self, **kwargs):
        """
        Метод исполняет вложенный файл задачи.
        Args:
            **kwargs: параметры для форматирования запроса
        """
        if Path(self.path_to_file).suffix == ".sql":
            self.etl_executor.execute_sql_queries_from(self.path_to_file, **kwargs)
        elif Path(self.path_to_file).suffix == ".py":
            exec(Path(self.path_to_file).read_text() % kwargs, {**globals(), "spark": self.etl_executor.spark})
        else:
            raise ValueError("Extension not in (.sql , .py)")
        return None

    @staticmethod
    def extract_all_tasks_by_pattern_with_step(path_to_scanning: str, pattern: str,
                                               etl_executor: EtlExecutor) -> List['TaskWithStep']:
        """
        Па узазанному пути извлекаются все задачи с шагом, удовлетовряющие шаблону.

        Важно, чтобы в названии самого файла скрипта для задачи было именно одно число - шаг.
        Например:
            корректные названия: stat1.tasks, 1stat.tasks, stat12.tasks
            не корректные названия: stat1_1.tasks, 12stat1.tasks

        Args:
            path_to_scanning: директория в которой нужно файлы скриптов искать (рекурсивно)
            pattern: шаблон интересующих файлов скриптов
            etl_executor: исполнитель запросов, содержащихся в файлах скриптов

        Returns:
            список задач

        Пример
            path_to_scanning:
                * "."
                * ".."
                * "/"
                * "/scripts"
                * "/user/home/scripts"

            pattern:
                * "step*.tasks"
                * "*.tasks"
        """
        all_files = []

        for file in Path(path_to_scanning).rglob(pattern):
            absolute_file_path = str(file.absolute())
            file_name = file.name

            step = re.findall(r"\d+", file_name)

            if len(step) != 1:
                raise ValueError("Expect file with one digit(step)")

            file_with_step = TaskWithStep(absolute_file_path, int(step[0]), etl_executor)
            all_files.append(file_with_step)
        return all_files

    @staticmethod
    def get_last_computed_step(table_name: str, is_recovery_mode, relevance_value: str,
                               etl_executor: EtlExecutor):
        """
        Находит последний выполненный шаг в соответствии с выбранным режимом.

        Args:
            is_recovery_mode: Параметр для выполнения восстановительного режима
            table_name: {схема таблицы}.{название таблицы} - полное название таблицы
            relevance_value: значение актуальности данных, для которого будет произведён поиск последнего актуального шага
            etl_executor: Объект для выполнения спарк задач
        Returns:
            Номер последнего выполненного шага
        """
        last_computed_step = 0
        if is_recovery_mode:
            last_ctl_validrom = etl_executor.get_max_value(table_name, "ctl_validfrom")
            last_computed_step = etl_executor.get_max_value(table_name=table_name,
                                                            col_name="last_computed_step",
                                                            condition="ctl_validfrom = '{}' and relevance_value = '{}'".
                                                            format(last_ctl_validrom, relevance_value))

        return last_computed_step
