-- Wave 1 | PG_SILVER.OU (6 cột) – SCD TYPE 1 | Sheet 'Organization Unit' | Track 3 – Fresher 3
{{ config(materialized='incremental', incremental_strategy='merge', unique_key=['OU_ID'], alias='ou') }}

with src as (
    select
        {{ fn_hash('T24_COMPANY', 'ou.RECID') }} as OU_ID,
        cast(ou.RECID as string)                 as OU_CODE,
        cast(ou.COMPANY_NAME as string)          as OU_NM
    from {{ bz('t24core_company') }} ou
)
{{ scd1(['OU_ID']) }}
