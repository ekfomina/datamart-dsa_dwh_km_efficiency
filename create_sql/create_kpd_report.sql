DROP TABLE IF EXISTS {kpd_report_table};
CREATE EXTERNAL TABLE IF NOT EXISTS {kpd_report_table}
(
calculated_table string,
ctl_loading int,
ctl_validfrom string,
relevance_value string,
is_force_load int,
count_rows bigint,
is_change int
)
STORED AS PARQUET
LOCATION "{kpd_report_location}"