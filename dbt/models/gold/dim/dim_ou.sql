-- Wave 3 | PG_GOLD.DIM_OU (6 cột) – SCD TYPE 2 | Sheet 'Dimension Organization Unit' | Track 3 – Fresher 3
-- Nguồn: PG_SILVER.OU ou (bản ghi hiệu lực)
-- DIM_KEY = CONCAT(TO_CHAR(ETL_CHANGE_DATE,'YYYYMMDD'), ou.OU_ID), ETL_CHANGE_DATE = COALESCE(SYS_UDT_DT, SYS_EFF_DT, ${ETL_DATE})
{{ config(materialized='incremental', incremental_strategy='merge', unique_key=['DIM_KEY'], alias='dim_ou') }}

with src as (
    select
        {{ dim_key(etl_change_date('ou'), 'ou.OU_ID') }} as DIM_KEY,
        ou.OU_ID              as OU_ID,
        ou.OU_CODE            as OU_CODE,
        ou.OU_NM              as OU_NM,
        ou.SYS_EFF_DT          as SYS_EFF_DT,
        cast(null as date)       as SYS_EXP_DT
    from {{ ref('ou') }} ou
    where ou.SYS_EXP_DT is null
)
{{ dim_scd2('OU_ID') }}
