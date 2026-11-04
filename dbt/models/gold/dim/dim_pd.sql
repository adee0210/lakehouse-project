-- Wave 3 | PG_GOLD.DIM_PD (6 cột) – SCD TYPE 2 | Sheet 'Dimension Product' | Track 3 – Fresher 3
-- Nguồn: PG_SILVER.PD pd (bản ghi hiệu lực)
-- DIM_KEY = CONCAT(TO_CHAR(ETL_CHANGE_DATE,'YYYYMMDD'), pd.PD_ID), ETL_CHANGE_DATE = COALESCE(SYS_UDT_DT, SYS_EFF_DT, ${ETL_DATE})
{{ config(materialized='incremental', incremental_strategy='merge', unique_key=['DIM_KEY'], alias='dim_pd') }}

with src as (
    select
        {{ dim_key(etl_change_date('pd'), 'pd.PD_ID') }} as DIM_KEY,
        pd.PD_ID              as PD_ID,
        pd.PD_CODE            as PD_CODE,
        pd.PD_NM              as PD_NM,
        pd.SYS_EFF_DT          as SYS_EFF_DT,
        cast(null as date)       as SYS_EXP_DT
    from {{ ref('pd') }} pd
    where pd.SYS_EXP_DT is null
)
{{ dim_scd2('PD_ID') }}
