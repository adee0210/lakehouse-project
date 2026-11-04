-- Wave 1 | PG_SILVER.EMPE (6 cột) – SCD TYPE 1 | Sheet 'Employee' | Track 3 – Fresher 3
{{ config(materialized='incremental', incremental_strategy='merge', unique_key=['EMPE_ID'], alias='empe') }}

with src as (
    select
        {{ fn_hash('T24_EMPLOYEE', 'empe.RECID') }} as EMPE_ID,
        cast(empe.RECID as string)                  as EMPE_CODE,
        cast(empe.NAME as string)                   as EMPE_NM
    from {{ bz('t24core_dept_acct_officer') }} empe
)
{{ scd1(['EMPE_ID']) }}
