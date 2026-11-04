-- Wave 2 | PG_SILVER.AR_X_IP_RLTNP (16 cột) – SCD TYPE 2, 3 nhánh UNION ALL | Sheet 'Arrangement x Involved Party' | Track 2
-- Quan hệ Hợp đồng vay x Bên liên quan: KH vay, Maker, Checker, RM giới thiệu, CB/TP đề xuất & quản lý khoản vay.
-- Phần biến đổi nằm ở stg_ar_x_ip_rltnp.sql (pg_silver_stg); model này chỉ áp dụng SCD2 (MERGE).
{{ config(materialized='incremental', incremental_strategy='merge', unique_key=['AR_ID', 'SYS_EFF_DT'], alias='ar_x_ip_rltnp') }}

with src as (select * from {{ ref('stg_ar_x_ip_rltnp') }})
{{ scd2(['AR_ID']) }}
