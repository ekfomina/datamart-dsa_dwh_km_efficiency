echo "start_bash"

# Extremely useful option allowing access to Hive Metastore from Spark 2.2
export HADOOP_CONF_DIR=$HADOOP_DIR_CONF

KEYTAB=$(basename $PATH_TO_KEYTAB)

set -e # Поток падает при прерывании дочернего процесса
echo "start get files from hdfs"
hdfs dfs -get ${PATH_TO_PROJ_HDFS}/scripts
hdfs dfs -get ${PATH_TO_PROJ_HDFS}/create_sql
hdfs dfs -get ${PATH_TO_PROJ_HDFS}/configs
set +e

# Preprocessing additional configs for launch of spark-submit
if [[ $SPARK_CONF == "-\\" ]]; then
SPARK_CONF="\\" # For backward compatibility
fi
SPARK_CONF=${SPARK_CONF:0:-1}

if [[ $USE_SPARK3 == "true" ]]; then
export SPARK_MAJOR_VERSION=3
SPARK_CONF="${SPARK_CONF} --conf spark.sql.parquet.writeLegacyFormat=true"
fi

echo SPARK_CONF = $SPARK_CONF

echo "start spark submit"
$COMMAND_SPARK \
$SPARK_CONF \
--master yarn \
--keytab $KEYTAB \
--queue $YARN_QUEUE \
--principal ${PRINCIPAL}@${REALM} \
--conf spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict \
--conf spark.sql.hive.convertMetastoreParquet=true \
--conf spark.pyspark.driver.python=$PATH_TO_PYTHON \
--conf spark.pyspark.python=$PATH_TO_PYTHON \
--executor-memory "$Y_EXECUTOR_MEM" \
--driver-memory "$Y_DRIVER_MEM" \
--executor-cores "$Y_EXECUTOR_CORES" \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.shuffle.service.enabled=true \
--conf spark.shuffle.useOldFetchProtocol=true \
--conf spark.sql.legacy.createHiveTableByDefault.enabled=true \
--conf spark.dynamicAllocation.minExecutors=$Y_MIN_EXECUTORS \
--conf spark.dynamicAllocation.maxExecutors=$Y_MAX_EXECUTORS \
--verbose \
scripts/main.py --etl_trgt_tbls "$ETL_TRGT_TBLS" \
                --etl_tbls_loc "$ETL_TBLS_LOC" \
                --trgt_lifetime_in_days "$TRGT_LIFETIME_IN_DAYS" \
                --parquet_size_in_blocks $PARQUET_SIZE_IN_BLOCKS \
                --etl_src_tbls "$ETL_SRC_TBLS" \
                --business_date_columns "$BUSINESS_DATE_COLUMNS" \
                --etl_force_load $ETL_FORCE_LOAD

spark_submit_exit_code=$?
echo "spark-submit exit code: ${spark_submit_exit_code}"

# если была ошибка то возвращяем не 0 код возврата
if [[ $spark_submit_exit_code != 0 ]]
then
  exit $spark_submit_exit_code
fi

echo "end_bash"
