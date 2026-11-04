-- Wave 1 | PG_SILVER.PD (7 cột) – SCD TYPE 1 | Sheet 'Product' | Track 3 – Fresher 3
-- Danh mục sản phẩm hợp nhất (UNION DISTINCT) từ:
--   (1) LOS_APP_PRODUCT.PRODUCT_CODE/PRODUCT_NAME
--   (2) FLEXBO_PGB_LOS_CONTRACT_FIELDS_TDATE.SAN_PHAM_CHO_VAY_KHCN / _KHDN
--   (3) T24CORE_LD_LOANS_AND_DEPOSITS.PRODUCTGR_CODE
-- => mọi PD_CODE xuất hiện trên LOAN_AR đều có trong DIM_PD.
{{ config(materialized='incremental', incremental_strategy='merge', unique_key=['PD_ID'], alias='pd') }}

with codes as (
    select PRODUCT_CODE as PD_CODE from {{ bz('los_app_product') }}
    union
    select SAN_PHAM_CHO_VAY_KHCN from {{ bz('flexbo_pgb_los_contract_fields_tdate') }}
    union
    select SAN_PHAM_CHO_VAY_KHDN from {{ bz('flexbo_pgb_los_contract_fields_tdate') }}
    union
    select PRODUCTGR_CODE from {{ bz('t24core_ld_loans_and_deposits') }}
),
los as (
    select PRODUCT_CODE, max(PRODUCT_NAME) as PRODUCT_NAME
    from {{ bz('los_app_product') }} group by PRODUCT_CODE
),
src as (
    select
        {{ fn_hash('T24_PRODUCT', 'pd.PD_CODE') }} as PD_ID,
        cast(pd.PD_CODE as string)                 as PD_CODE,
        cast(los.PRODUCT_NAME as string)           as PD_NM,
        'T24_PRODUCT'                              as SRC_STM_CODE
    from codes pd
    left join los on pd.PD_CODE = los.PRODUCT_CODE
    where pd.PD_CODE is not null
)
{{ scd1(['PD_ID']) }}
