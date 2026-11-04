-- Wave 2 | PG_SILVER.AR_AVY_SHD_ITM (19 cột) – SCD TYPE 2 | Sheet 'Arrangement Activity Schedule I' | Track 4 – Fresher 4
-- Dòng lịch thanh toán bóc từ bút toán tương lai STMT_ENTRY: RECID LIKE 'F%' AND nvl(COMMIT_ACTION, '_') <> 'D' AND BOOKING_DATE = ${ETL_DATE}
-- contract_id: LD% -> OUR_REFERENCE ; PD% -> SUBSTR(OUR_REFERENCE, 3)
-- pdp: kỳ quá hạn gần nhất = PAYMENT_DTE_DUE = MAX(PREV_SCHED_DATE <= ${ETL_DATE}) – khử trùng 1 dòng / CONTRACT_ID.
-- Phần biến đổi nằm ở stg_ar_avy_shd_itm.sql (pg_silver_stg); model này chỉ áp dụng SCD2 (MERGE).
{{ config(materialized='incremental', incremental_strategy='merge', unique_key=['AR_AVY_SHD_ID', 'SYS_EFF_DT'], alias='ar_avy_shd_itm') }}

with src as (select * from {{ ref('stg_ar_avy_shd_itm') }})
{{ scd2(['AR_AVY_SHD_ID']) }}
