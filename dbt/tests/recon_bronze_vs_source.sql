-- Wave 0: số dòng PG_BRONZE (ETL_DATE = ${ETL_DATE}) khớp 1:1 với bảng nguồn – 28 bảng (sinh tự động)
select 'EBANKING_COL_UDF_VALUE' as tbl, b.n as bronze_rows, s.n as source_rows
from (select count(*) as n from {{ ref('ebanking_col_udf_value') }} where ETL_DATE = {{ etl_date() }}) b,
     (select count(*) as n from {{ source('ebanking', 'col_udf_value') }}) s
where b.n <> s.n
union all
select 'FLEXBO_PGBLD_CONTRACT_UDFIELD_HIST' as tbl, b.n as bronze_rows, s.n as source_rows
from (select count(*) as n from {{ ref('flexbo_pgbld_contract_udfield_hist') }} where ETL_DATE = {{ etl_date() }}) b,
     (select count(*) as n from {{ source('flexbo', 'pgbld_contract_udfield_hist') }}) s
where b.n <> s.n
union all
select 'FLEXBO_PGBLD_RT_CONTRACT_UDFIELD_HIST' as tbl, b.n as bronze_rows, s.n as source_rows
from (select count(*) as n from {{ ref('flexbo_pgbld_rt_contract_udfield_hist') }} where ETL_DATE = {{ etl_date() }}) b,
     (select count(*) as n from {{ source('flexbo', 'pgbld_rt_contract_udfield_hist') }}) s
where b.n <> s.n
union all
select 'FLEXBO_PGB_CONTRACT_UDF_MAP' as tbl, b.n as bronze_rows, s.n as source_rows
from (select count(*) as n from {{ ref('flexbo_pgb_contract_udf_map') }} where ETL_DATE = {{ etl_date() }}) b,
     (select count(*) as n from {{ source('flexbo', 'pgb_contract_udf_map') }}) s
where b.n <> s.n
union all
select 'FLEXBO_PGB_LDTB_CONTRACT_MASTER' as tbl, b.n as bronze_rows, s.n as source_rows
from (select count(*) as n from {{ ref('flexbo_pgb_ldtb_contract_master') }} where ETL_DATE = {{ etl_date() }}) b,
     (select count(*) as n from {{ source('flexbo', 'pgb_ldtb_contract_master') }}) s
where b.n <> s.n
union all
select 'FLEXBO_PGB_LOS_CONTRACT_FIELDS_TDATE' as tbl, b.n as bronze_rows, s.n as source_rows
from (select count(*) as n from {{ ref('flexbo_pgb_los_contract_fields_tdate') }} where ETL_DATE = {{ etl_date() }}) b,
     (select count(*) as n from {{ source('flexbo', 'pgb_los_contract_fields_tdate') }}) s
where b.n <> s.n
union all
select 'LOS_APP_FACILITY' as tbl, b.n as bronze_rows, s.n as source_rows
from (select count(*) as n from {{ ref('los_app_facility') }} where ETL_DATE = {{ etl_date() }}) b,
     (select count(*) as n from {{ source('los_app', 'facility') }}) s
where b.n <> s.n
union all
select 'LOS_APP_LOAN_DISBURSEMENT' as tbl, b.n as bronze_rows, s.n as source_rows
from (select count(*) as n from {{ ref('los_app_loan_disbursement') }} where ETL_DATE = {{ etl_date() }}) b,
     (select count(*) as n from {{ source('los_app', 'loan_disbursement') }}) s
where b.n <> s.n
union all
select 'LOS_APP_PRODUCT' as tbl, b.n as bronze_rows, s.n as source_rows
from (select count(*) as n from {{ ref('los_app_product') }} where ETL_DATE = {{ etl_date() }}) b,
     (select count(*) as n from {{ source('los_app', 'product') }}) s
where b.n <> s.n
union all
select 'SMY_STMT_DAILY' as tbl, b.n as bronze_rows, s.n as source_rows
from (select count(*) as n from {{ ref('smy_stmt_daily') }} where ETL_DATE = {{ etl_date() }}) b,
     (select count(*) as n from {{ source('smy', 'stmt_daily') }}) s
where b.n <> s.n
union all
select 'SOURCE_SAOKE_CRB' as tbl, b.n as bronze_rows, s.n as source_rows
from (select count(*) as n from {{ ref('source_saoke_crb') }} where ETL_DATE = {{ etl_date() }}) b,
     (select count(*) as n from {{ source('source', 'saoke_crb') }}) s
where b.n <> s.n
union all
select 'SOURCE_SAOKE_MVMT' as tbl, b.n as bronze_rows, s.n as source_rows
from (select count(*) as n from {{ ref('source_saoke_mvmt') }} where ETL_DATE = {{ etl_date() }}) b,
     (select count(*) as n from {{ source('source', 'saoke_mvmt') }}) s
where b.n <> s.n
union all
select 'T24CORE_COMPANY' as tbl, b.n as bronze_rows, s.n as source_rows
from (select count(*) as n from {{ ref('t24core_company') }} where ETL_DATE = {{ etl_date() }}) b,
     (select count(*) as n from {{ source('t24core', 'company') }}) s
where b.n <> s.n
union all
select 'T24CORE_CURRENCY' as tbl, b.n as bronze_rows, s.n as source_rows
from (select count(*) as n from {{ ref('t24core_currency') }} where ETL_DATE = {{ etl_date() }}) b,
     (select count(*) as n from {{ source('t24core', 'currency') }}) s
where b.n <> s.n
union all
select 'T24CORE_CURRENCY_HIS' as tbl, b.n as bronze_rows, s.n as source_rows
from (select count(*) as n from {{ ref('t24core_currency_his') }} where ETL_DATE = {{ etl_date() }}) b,
     (select count(*) as n from {{ source('t24core', 'currency_his') }}) s
where b.n <> s.n
union all
select 'T24CORE_CUSTOMER' as tbl, b.n as bronze_rows, s.n as source_rows
from (select count(*) as n from {{ ref('t24core_customer') }} where ETL_DATE = {{ etl_date() }}) b,
     (select count(*) as n from {{ source('t24core', 'customer') }}) s
where b.n <> s.n
union all
select 'T24CORE_DEPT_ACCT_OFFICER' as tbl, b.n as bronze_rows, s.n as source_rows
from (select count(*) as n from {{ ref('t24core_dept_acct_officer') }} where ETL_DATE = {{ etl_date() }}) b,
     (select count(*) as n from {{ source('t24core', 'dept_acct_officer') }}) s
where b.n <> s.n
union all
select 'T24CORE_LD_LOANS_AND_DEPOSITS' as tbl, b.n as bronze_rows, s.n as source_rows
from (select count(*) as n from {{ ref('t24core_ld_loans_and_deposits') }} where ETL_DATE = {{ etl_date() }}) b,
     (select count(*) as n from {{ source('t24core', 'ld_loans_and_deposits') }}) s
where b.n <> s.n
union all
select 'T24CORE_LD_LOANS_AND_DEPOSITS_HIS' as tbl, b.n as bronze_rows, s.n as source_rows
from (select count(*) as n from {{ ref('t24core_ld_loans_and_deposits_his') }} where ETL_DATE = {{ etl_date() }}) b,
     (select count(*) as n from {{ source('t24core', 'ld_loans_and_deposits_his') }}) s
where b.n <> s.n
union all
select 'T24CORE_LD_SCHEDULE_DEFINE_MV' as tbl, b.n as bronze_rows, s.n as source_rows
from (select count(*) as n from {{ ref('t24core_ld_schedule_define_mv') }} where ETL_DATE = {{ etl_date() }}) b,
     (select count(*) as n from {{ source('t24core', 'ld_schedule_define_mv') }}) s
where b.n <> s.n
union all
select 'T24CORE_LIMIT' as tbl, b.n as bronze_rows, s.n as source_rows
from (select count(*) as n from {{ ref('t24core_limit') }} where ETL_DATE = {{ etl_date() }}) b,
     (select count(*) as n from {{ source('t24core', 'limit') }}) s
where b.n <> s.n
union all
select 'T24CORE_MB_MG_SAVING_MULTI' as tbl, b.n as bronze_rows, s.n as source_rows
from (select count(*) as n from {{ ref('t24core_mb_mg_saving_multi') }} where ETL_DATE = {{ etl_date() }}) b,
     (select count(*) as n from {{ source('t24core', 'mb_mg_saving_multi') }}) s
where b.n <> s.n
union all
select 'T24CORE_PD_PAYMENT_DUE' as tbl, b.n as bronze_rows, s.n as source_rows
from (select count(*) as n from {{ ref('t24core_pd_payment_due') }} where ETL_DATE = {{ etl_date() }}) b,
     (select count(*) as n from {{ source('t24core', 'pd_payment_due') }}) s
where b.n <> s.n
union all
select 'T24CORE_PD_PAYMENT_DUE_HIS' as tbl, b.n as bronze_rows, s.n as source_rows
from (select count(*) as n from {{ ref('t24core_pd_payment_due_his') }} where ETL_DATE = {{ etl_date() }}) b,
     (select count(*) as n from {{ source('t24core', 'pd_payment_due_his') }}) s
where b.n <> s.n
union all
select 'T24CORE_PD_PAYMENT_DUE_HIS_MV' as tbl, b.n as bronze_rows, s.n as source_rows
from (select count(*) as n from {{ ref('t24core_pd_payment_due_his_mv') }} where ETL_DATE = {{ etl_date() }}) b,
     (select count(*) as n from {{ source('t24core', 'pd_payment_due_his_mv') }}) s
where b.n <> s.n
union all
select 'T24CORE_PD_PAYMENT_DUE_MV' as tbl, b.n as bronze_rows, s.n as source_rows
from (select count(*) as n from {{ ref('t24core_pd_payment_due_mv') }} where ETL_DATE = {{ etl_date() }}) b,
     (select count(*) as n from {{ source('t24core', 'pd_payment_due_mv') }}) s
where b.n <> s.n
union all
select 'T24CORE_SECTOR' as tbl, b.n as bronze_rows, s.n as source_rows
from (select count(*) as n from {{ ref('t24core_sector') }} where ETL_DATE = {{ etl_date() }}) b,
     (select count(*) as n from {{ source('t24core', 'sector') }}) s
where b.n <> s.n
union all
select 'T24CORE_STMT_ENTRY' as tbl, b.n as bronze_rows, s.n as source_rows
from (select count(*) as n from {{ ref('t24core_stmt_entry') }} where ETL_DATE = {{ etl_date() }}) b,
     (select count(*) as n from {{ source('t24core', 'stmt_entry') }}) s
where b.n <> s.n
