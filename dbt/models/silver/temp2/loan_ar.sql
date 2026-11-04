-- Wave 2 | PG_SILVER.LOAN_AR (31 cột) – SCD TYPE 2, 3 nhánh UNION ALL | Sheet 'Loan Arrangement' | Track 2 – Fresher 2
-- Nhánh 1: LD hiện hành | Nhánh 2: LD_HIS (đã tất toán) | Nhánh 3: PD_PAYMENT_DUE (quá hạn, AR_CODE = SUBSTR(RECID,1,14))
-- FRST_OU_CODE: theo ghi chú BA, tính FIRST_VALUE(CO_CODE) trên tập hợp nhất LD ∪ LD_HIS
--               (ORDER BY VALUE_DATE, CURR_NO) để lấy chi nhánh mở đầu tiên xuyên suốt lịch sử HĐ.
-- LMT_ID: chỉ băm khi LIMIT_REFERENCE IS NOT NULL.
-- Phần biến đổi nằm ở stg_loan_ar.sql (pg_silver_stg); model này chỉ áp dụng SCD2 (MERGE).
{{ config(materialized='incremental', incremental_strategy='merge', unique_key=['AR_ID', 'SYS_EFF_DT'], alias='loan_ar') }}

with src as (select * from {{ ref('stg_loan_ar') }})
{{ scd2(['AR_ID']) }}
