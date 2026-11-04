-- Wave 0 | LOS_APP.PRODUCT@TO_LOS -> PG_BRONZE.LOS_APP_PRODUCT
-- 1:1 Raw Mirror (4 cột nguồn) + 3 cột Audit: ETL_DATE, INGEST_TMS, SRC_STM_CODE
-- Job_dependence: sql/bronze/los_app_product.sql | schedule 30 0 * * *
{{ config(alias='los_app_product') }}

select
    src.*,
    {{ etl_date() }} as ETL_DATE,
    current_timestamp() as INGEST_TMS,
    'LOS_APP_PRODUCT' as SRC_STM_CODE
from {{ source('los_app', 'product') }} src
