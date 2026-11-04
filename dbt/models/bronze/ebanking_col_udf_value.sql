-- Wave 0 | EBANKING.COL_UDF_VALUE@PGDB -> PG_BRONZE.EBANKING_COL_UDF_VALUE
-- 1:1 Raw Mirror (4 cột nguồn) + 3 cột Audit: ETL_DATE, INGEST_TMS, SRC_STM_CODE
-- Job_dependence: sql/bronze/ebanking_col_udf_value.sql | schedule 30 0 * * *
{{ config(alias='ebanking_col_udf_value') }}

select
    src.*,
    {{ etl_date() }} as ETL_DATE,
    current_timestamp() as INGEST_TMS,
    'EBANKING_COL_UDF_VALUE' as SRC_STM_CODE
from {{ source('ebanking', 'col_udf_value') }} src
