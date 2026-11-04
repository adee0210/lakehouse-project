-- Wave 3 | PG_GOLD.DIM_LMT (6 cột) – SCD TYPE 2 | Sheet 'Dimension Limit' | Track 3 – Fresher 3
-- Nguồn: PG_SILVER.LMT l (bản ghi hiệu lực)
-- DIM_KEY = CONCAT(TO_CHAR(ETL_CHANGE_DATE,'YYYYMMDD'), l.LMT_ID), ETL_CHANGE_DATE = COALESCE(SYS_UDT_DT, SYS_EFF_DT, ${ETL_DATE})
{{ config(materialized='incremental', incremental_strategy='merge', unique_key=['DIM_KEY'], alias='dim_lmt') }}

with src as (
    select
        {{ dim_key(etl_change_date('l'), 'l.LMT_ID') }} as DIM_KEY,
        l.LMT_ID             as LMT_ID,
        l.LMT_CODE           as LMT_CODE,
        l.LMT_DSC            as LMT_DSC,
        l.SYS_EFF_DT          as SYS_EFF_DT,
        cast(null as date)       as SYS_EXP_DT
    from {{ ref('lmt') }} l
    where l.SYS_EXP_DT is null
)
{{ dim_scd2('LMT_ID') }}
