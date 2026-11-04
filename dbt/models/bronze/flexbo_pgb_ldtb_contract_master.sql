-- Wave 0 | FLEXBO.PGB_LDTB_CONTRACT_MASTER -> PG_BRONZE.FLEXBO_PGB_LDTB_CONTRACT_MASTER
-- 1:1 Raw Mirror (3 cột nguồn) + 3 cột Audit: ETL_DATE, INGEST_TMS, SRC_STM_CODE
-- Job_dependence: sql/bronze/flexbo_pgb_ldtb_contract_master.sql | schedule 30 0 * * *
{{ config(alias='flexbo_pgb_ldtb_contract_master') }}

select
    src.*,
    {{ etl_date() }} as ETL_DATE,
    current_timestamp() as INGEST_TMS,
    'FLEXBO_PGB_LDTB_CONTRACT_MASTER' as SRC_STM_CODE
from {{ source('flexbo', 'pgb_ldtb_contract_master') }} src
