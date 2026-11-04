-- Wave 1 | PG_SILVER.AR_RATE_HIST (12 cột) – SCD TYPE 2, 3 nhánh UNION ALL | Sheet 'Arrangement Rate History' | Track 6
-- Nhánh 1: LD hiện hành | Nhánh 2: LD_HIS (version cuối, không REVE, không còn trong LD) | Nhánh 3: PD quá hạn
-- AR_CODE chuẩn hoá: nhánh 2 SUBSTR(RECID,1,INSTR(RECID,';')-1), nhánh 3 SUBSTR(d.RECID,1,14)
-- Phần biến đổi nằm ở stg_ar_rate_hist.sql (pg_silver_stg); model này chỉ áp dụng SCD2 (MERGE).
{{ config(materialized='incremental', incremental_strategy='merge', unique_key=['AR_ID', 'SYS_EFF_DT'], alias='ar_rate_hist') }}

with src as (select * from {{ ref('stg_ar_rate_hist') }})
{{ scd2(['AR_ID']) }}
