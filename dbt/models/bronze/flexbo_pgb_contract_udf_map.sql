-- Wave 0 | FLEXBO.PGB_CONTRACT_UDF_MAP -> PG_BRONZE.FLEXBO_PGB_CONTRACT_UDF_MAP
-- 1:1 Raw Mirror (3 cột nguồn) + 3 cột Audit: ETL_DATE, INGEST_TMS, SRC_STM_CODE
-- Job_dependence: sql/bronze/flexbo_pgb_contract_udf_map.sql | schedule 30 0 * * *
{{ config(alias='flexbo_pgb_contract_udf_map') }}

select
    src.*,
    {{ etl_date() }} as ETL_DATE,
    current_timestamp() as INGEST_TMS,
    'FLEXBO_PGB_CONTRACT_UDF_MAP' as SRC_STM_CODE
from {{ source('flexbo', 'pgb_contract_udf_map') }} src
