-- Wave 1 | PG_SILVER.CCY (8 cột) – SCD TYPE 1 | Sheet 'Currency' | Track 3 – Fresher 3
-- Nguồn: PG_BRONZE.T24CORE_CURRENCY cur
{{ config(materialized='incremental', incremental_strategy='merge', unique_key=['CCY_ID'], alias='ccy') }}

with src as (
    select
        {{ fn_hash('T24_CURRENCY', 'cur.ID') }}  as CCY_ID,
        cast(cur.ID as string)                   as CCY_CODE,
        cast(cur.CCY_NAME as string)             as CCY_NM,
        cast(cur.NUMERIC_CCY_CODE as string)     as NUMERIC_CCY_CODE,
        'T24_CURRENCY'                           as SRC_STM_CODE
    from {{ bz('t24core_currency') }} cur
)
{{ scd1(['CCY_ID']) }}
