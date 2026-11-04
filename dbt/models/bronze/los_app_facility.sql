-- Wave 0 | LOS_APP.FACILITY@TO_LOS -> PG_BRONZE.LOS_APP_FACILITY
-- 1:1 Raw Mirror (2 cột nguồn) + 3 cột Audit: ETL_DATE, INGEST_TMS, SRC_STM_CODE
-- Job_dependence: sql/bronze/los_app_facility.sql | schedule 30 0 * * *
{{ config(alias='los_app_facility') }}

select
    src.*,
    {{ etl_date() }} as ETL_DATE,
    current_timestamp() as INGEST_TMS,
    'LOS_APP_FACILITY' as SRC_STM_CODE
from {{ source('los_app', 'facility') }} src
