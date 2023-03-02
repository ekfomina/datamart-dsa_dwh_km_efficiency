import logging
import os

from utils import dfs, log, etl, statistics, parser

logger = logging.getLogger(__name__)


if __name__ == '__main__':
    """
    1. Инициализация аргументов, логера, статистик потока, интерфейсов для ETL задач и работы с HDFS
    """
    query_executor = etl.EtlExecutor("{}: {}".format(os.environ.get("WF_NAME"), os.environ.get('LOADING_ID')))
    log.setup_logger("configs/logger_config.yaml")

    logger.ctl_info("Spark session was launched on version {}".format(query_executor.spark.version))
    logger.ctl_info("Path to logs in HDFS: {0}/{1}".format(os.environ.get("PATH_TO_LOGS_IN_HDFS"),
                                                           log.get_log_file_name()))
    args = parser.parse_arguments()
    logger.debug(str(vars(args)))

    stats = statistics.Statistics(entity_id=int(os.environ.get('CTL_ENTITY_ID')),
                                  loading_id=int(os.environ.get('LOADING_ID')),
                                  application_id=query_executor.spark.sparkContext.applicationId,
                                  relevance_value=args.relevance_value)
    if args.relevance_value == "-":
        stats.relevance_value = query_executor.get_stat_value_from("scripts/tasks/compute_relevance_value.sql",
                                                                   **args.etl_src_tbls)

    file_system = dfs.HadoopFileSystem(query_executor.spark.sparkContext)
    partition_control = dfs.PartitionController(query_executor, file_system, args.parquet_size_in_blocks)

    """
    2. Режим архивной загрузки и обновление метанинформации о таблицах
    """
    if args.etl_force_load:
        logger.ctl_info("Force load is True. Start to delete all target partitions")

    tables_info = {}
    for key, table_name in args.etl_trgt_tbls.items():
        tables_info[key + "_table"] = table_name
        tables_info[key + "_location"] = args.etl_tbls_loc[table_name]
        if args.etl_force_load and key != "kpd_report":
            # В режиме архивной загрузки отладочная таблица kpd_report сохраняет свои записи
            file_system.clear_directory(args.etl_tbls_loc[table_name], keep_directory=True)

    logger.ctl_info("Recreate tables")
    query_executor.execute_sql_queries_from('create_sql/create_*.sql', **tables_info)

    """
    3. Перенос данных между таблицами
    """
    transferred_table = args.etl_trgt_tbls["to_transfer"]
    logger.ctl_info("Start of datamart transfer")
    if stats.relevance_value:
        # Get columns of target table
        columns = query_executor.get_table_scheme(transferred_table)
        query_executor.transfer_table(args.etl_src_tbls["to_transfer"], transferred_table, columns,
                                      ctl_loading=stats.loading_id, ctl_validfrom=stats.ctl_validfrom)
        logger.ctl_info("Data was transferred")
    else:
        logger.ctl_info("There aren't data in sources")
    stats.add_rows_number(1, transferred_table, query_executor.cnt_rows(transferred_table))

    """
    4. Репартиционариование данных таблиц и сбор статистик
    """
    logger.ctl_info("Repart {}".format(transferred_table))
    partition_control.repart(transferred_table,
                             start_date=stats.ctl_validfrom)
    logger.ctl_info("Delete old partitions {}".format(transferred_table))
    partition_control.delete_outdated_partitions(
        table_name=transferred_table,
        lifetime_in_days=args.trgt_lifetime_in_days[transferred_table],
        curr_date=stats.ctl_validfrom
    )

    table_last_partition = query_executor.get_last_partition(transferred_table)
    relevance_column = args.business_date_columns[transferred_table]
    business_date = table_last_partition.get(relevance_column,
                                             query_executor.get_max_value(transferred_table, relevance_column))
    stats.update_business_date(business_date)
    table_last_partition_filter = " and ".join(["{} = '{}'".format(name, value)
                                                for name, value in table_last_partition.items()])
    stats.update_max_cdc_date(query_executor.get_max_value(table_name=transferred_table,
                                                           col_name="ctl_validfrom",
                                                           condition=table_last_partition_filter))

    """
    5. Запись статистик в KPD report и в CTL
    """
    _, calc_table_name, rows_number = stats.table_rows_number_arr[0]
    last_mod = file_system.get_modification_time(query_executor.get_hdfs_location(calc_table_name))
    stats.change = int(stats.ctl_validfrom < last_mod or args.etl_force_load)
    query_executor.insert_single_row_data(table_name=args.etl_trgt_tbls["kpd_report"],
                                          calculated_table=calc_table_name,
                                          ctl_loading=stats.loading_id,
                                          ctl_validfrom=stats.ctl_validfrom,
                                          relevance_value=stats.relevance_value,
                                          is_force_load=args.etl_force_load,
                                          count_rows=rows_number,
                                          is_change=stats.change)
    partition_control.repart(args.etl_trgt_tbls["kpd_report"])
    logger.ctl_info("Kpd report has been updated!")

    statistics.CTLStatisticsServer.get_server_upload_statistics(os.environ.get('CTL'), stats)
    logger.info("End python")
