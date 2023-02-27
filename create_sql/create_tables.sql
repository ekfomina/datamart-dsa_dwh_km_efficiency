DROP TABLE IF EXISTS {to_transfer_table};
CREATE TABLE IF NOT EXISTS {to_transfer_table}
(
    ctl_loading                         bigint comment "id загрузки",
    ctl_validfrom                       string comment "дата загрузки",
    src_ctl_loading                     bigint comment "id изначальной загрузки из Greenplum в Hadoop",
    transaction_id                      bigint comment "уникальный идентификатор операции (техническое поле при перекладке из GP в пром Хадуп - необходимо для отслеживания проблем с данными при перекладке)",
    product_cd                          string comment "название продукта",
    saphr_id                            bigint comment "табельный номер",
    age                                 double comment "возраст",
    sick_leave_without_pay_m_qty        bigint comment "кол-во дней отпуска по болезни без оплаты за месяц",
    sick_leave_without_pay_y_qty        double comment "кол-во дней отпуска по болезни без оплаты за год",
    sick_leave_m_qty                    bigint comment "кол-во дней отпуска по болезни за месяц",
    sick_leave_y_qty                    double comment "кол-во дней отпуска по болезни за год",
    boss_age_y_qty                      double comment "возраст руководителя",
    diff_age_with_boss_qty              double comment "разница в возрасте с руководителем",
    education_id                        double comment "образование",
    epk_id                              bigint comment "ЕПК",
    fact_worktime_hour_val              double comment "кол-во фактически отработанного времени",
    gender                              string comment "пол",
    gosb_code                           string comment "ГОСБ",
    is_urm                              double comment "флаг удалённого доступа",
    business_trip_y_qty                 double comment "кол-во дней в командировке за год",
    cont_post_experience_day            double comment "кол-во дней непрерывного рабочего стажа на позиции",
    cont_work_y_boss_qty                double comment "кол-во дней непрерывного рабочего стажа у руководителя",
    absent_without_reason_m_qty         bigint comment "кол-во дней отсутствий без уважительной причины за месяц",
    org_level                           double comment "уровень организационной структуры",
    org_type_level                      bigint comment "уровень типовой организационной структуры",
    vacation_without_pay_y_qty          double comment "кол-во дней отпуска без оплаты за год",
    vacation_m_qty                      double comment "кол-во дней отпуска за месяц",
    child_care_vacation_m_qty           double comment "кол-во дней отпуска по уходу за ребёнком за месяц",
    child_care_vacation_y_qty           double comment "кол-во дней отпуска по уходу за ребёнком за год",
    vacation_y_qty                      double comment "кол-во дней отпуска за год",
    absent_for_good_reason_y_qty        double comment "кол-во дней отсутствий по уважительным причинам за год",
    plan_fact_worktime_hour_val         double comment "отношение фактически отработанного времени к нормативному",
    plan_worktime_hour_val              double comment "нормативный рабочий график",
    grade_num                           double comment "грейд",
    post_experience_day                 double comment "рабочий стаж на позиции",
    pos_name                            string comment "краткое наименование должности",
    position_name                       string comment "полное наименование должности",
    prev_3_months_share_product_per_tab double comment "доля продаж продукта в общих продажах сотрудника за предыдущие 3 месяца",
    prev_month_share_product_per_tab    double comment "доля продаж продукта в общих продажах сотрудника за предыдущий месяц",
    prev_months_event_rate              double comment "средний уровень продаж продукта за предыдущий месяц",
    sb_cont_experience_day_qty          double comment "кол-во дней непрерывного рабочего стажа в Сбере",
    sb_experience_day_qty               double comment "кол-во дней рабочего стажа в Сбере",
    tb_name                             string comment "название ТБ",
    tb_code                             string comment "номер ТБ",
    city                                string comment "город",
    study_vacation_m_qty                bigint comment "кол-во дней учебного отпуска за месяц",
    work_y_with_boss_qty                double comment "рабочий стаж работы с руководителем",
    work_experience_day_qty             double comment "рабочий стаж, дней"
)
PARTITIONED BY (
    report_dt                           string comment "отчётный период"
)
STORED AS PARQUET
LOCATION "{to_transfer_location}";

MSCK REPAIR TABLE {to_transfer_table}
