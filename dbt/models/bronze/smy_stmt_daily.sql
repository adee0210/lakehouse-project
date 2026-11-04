-- Wave 0 | SMY.STMT_DAILY -> PG_BRONZE.SMY_STMT_DAILY
-- 1:1 Raw Mirror (56 cột nguồn) + 3 cột Audit: ETL_DATE, INGEST_TMS, SRC_STM_CODE
-- Job_dependence: sql/bronze/smy_stmt_daily.sql | schedule 30 0 * * *
{{ config(alias='smy_stmt_daily') }}

select
    src.*,
    {{ etl_date() }} as ETL_DATE,
    current_timestamp() as INGEST_TMS,
    'SMY_STMT_DAILY' as SRC_STM_CODE
from {{ source('smy', 'stmt_daily') }} src
