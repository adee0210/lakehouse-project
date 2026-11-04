-- Wave 2 | PG_SILVER.LOAN_AR_PRFL (37 cột) – SCD TYPE 2, 3 nhánh UNION ALL | Sheet 'Loan Arrangement Profile' | Track 2
-- 37 cột đồng bộ 100% thứ tự & kiểu dữ liệu giữa 3 nhánh.
-- Chống fan-out: bảng phụ (b,e,f,g,h) lấy 1 dòng / khoá join; h = DISTINCT TRANS_REF, SBV_CODE LIKE '39%'.
-- INT_LQD_AR_ID / PNP_LQD_AR_ID / GL_ITM_ID: chỉ băm khi cột nguồn IS NOT NULL.
-- Phần biến đổi nằm ở stg_loan_ar_prfl.sql (pg_silver_stg); model này chỉ áp dụng SCD2 (MERGE).
{{ config(materialized='incremental', incremental_strategy='merge', unique_key=['AR_ID', 'SYS_EFF_DT'], alias='loan_ar_prfl') }}

with src as (select * from {{ ref('stg_loan_ar_prfl') }})
{{ scd2(['AR_ID']) }}
