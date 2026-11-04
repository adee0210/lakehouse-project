-- Wave 0 | FLEXBO.PGB_LOS_CONTRACT_FIELDS_TDATE -> PG_BRONZE.FLEXBO_PGB_LOS_CONTRACT_FIELDS_TDATE
-- 1:1 Raw Mirror (18 cột nguồn) + 3 cột Audit: ETL_DATE, INGEST_TMS, SRC_STM_CODE
-- Job_dependence: sql/bronze/flexbo_pgb_los_contract_fields_tdate.sql | schedule 30 0 * * *
{{ config(alias='flexbo_pgb_los_contract_fields_tdate') }}

select
    src.*,
    {{ etl_date() }} as ETL_DATE,
    current_timestamp() as INGEST_TMS,
    'FLEXBO_PGB_LOS_CONTRACT_FIELDS_TDATE' as SRC_STM_CODE
from {{ source('flexbo', 'pgb_los_contract_fields_tdate') }} src
