-- Wave 0 | T24CORE.LD_SCHEDULE_DEFINE_MV -> PG_BRONZE.T24CORE_LD_SCHEDULE_DEFINE_MV
-- 1:1 Raw Mirror (2 cột nguồn) + 3 cột Audit: ETL_DATE, INGEST_TMS, SRC_STM_CODE
-- Job_dependence: sql/bronze/t24core_ld_schedule_define_mv.sql | schedule 30 0 * * *
{{ config(alias='t24core_ld_schedule_define_mv') }}

select
    src.*,
    {{ etl_date() }} as ETL_DATE,
    current_timestamp() as INGEST_TMS,
    'T24CORE_LD_SCHEDULE_DEFINE_MV' as SRC_STM_CODE
from {{ source('t24core', 'ld_schedule_define_mv') }} src
