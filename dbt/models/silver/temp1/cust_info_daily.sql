-- Wave 1 | PG_SILVER.CUST_INFO_DAILY – SNAPSHOT (Daily Customer & Contract Info)
-- Sheet 'CUST_INFO_DAILY' + Map_SRC2TRG: 63 cột. main = SMY_STMT_DAILY (stmt, BOOKING_DATE = ${ETL_DATE}) + 9 bảng phụ.
-- Grain: 1 dòng / bút toán stmt x field UDF (udf = bảng EAV PGB_CONTRACT_UDF_MAP, giữ nguyên theo thiết kế).
-- m: version mới nhất của hợp đồng FCC; các bảng UDF lịch sử lấy bản ghi mới nhất theo hợp đồng.
{{ config(materialized='incremental', incremental_strategy='append', partition_by=['ETL_DATE'],
          pre_hook="{{ delete_partition('ETL_DATE') }}", alias='cust_info_daily') }}

with stmt as (
    select * from {{ bz('smy_stmt_daily') }} where {{ t24_date('BOOKING_DATE') }} = {{ etl_date() }}
),
m as (
    select * from (
        select x.*, row_number() over (partition by x.CONTRACT_REF_NO order by x.VERSION_NO desc) as rn
        from {{ bz('flexbo_pgb_ldtb_contract_master') }} x
    ) where rn = 1
),
td as (select * from {{ bz('flexbo_pgb_los_contract_fields_tdate') }} where {{ t24_date('MOV_DATE') }} = {{ etl_date() }}),
udf as (select * from {{ bz('flexbo_pgb_contract_udf_map') }}),
udf_h as (
    select * from (
        select x.*, row_number() over (partition by x.CONTRACT_REF_NO order by x.MOV_DATE desc) as rn
        from {{ bz('flexbo_pgbld_contract_udfield_hist') }} x
    ) where rn = 1
),
rt_h as (
    select * from (
        select x.*, row_number() over (partition by x.CONTRACT_REF_NO order by x.NGAY_HET_COVID19 desc) as rn
        from {{ bz('flexbo_pgbld_rt_contract_udfield_hist') }} x
    ) where rn = 1
),
col_udf as (select * from {{ bz('ebanking_col_udf_value') }} where UDF_ID = 'NHANNO_BATBUOC'),
ld_los as (
    select * from {{ bz('los_app_loan_disbursement') }} where AMND_STATE = 'F' and STATUS in ('A', 'U')
),
fac as (select * from {{ bz('los_app_facility') }}),
pd_los as (select * from {{ bz('los_app_product') }})
select
    TRIM(col_udf.COL_REF) as COL_REF,
    TRIM(col_udf.FUNCTION_CODE) as FUNCTION_CODE,
    TRIM(col_udf.UDF_VALUE) as UDF_VALUE,
    TRIM(udf.CONTRACT_REF_NO) as UDF_MAP_CONTRACT_REF_NO,
    TRIM(udf.FIELD_NAME) as FIELD_NAME,
    TRIM(udf.FIELD_VAL) as FIELD_VAL,
    {{ t24_date('m.BOOKING_DATE') }} as FCC_BOOKING_DATE,
    TRIM(m.CONTRACT_REF_NO) as FCC_CONTRACT_REF_NO,
    COALESCE(CAST(m.VERSION_NO AS DECIMAL(10)), 0) as VERSION_NO,
    TRIM(td.CAN_BO_DE_XUAT_GN) as CAN_BO_DE_XUAT_GN,
    TRIM(td.CAR_APPLICATION_CODE) as CAR_APPLICATION_CODE,
    TRIM(td.CONTRACT_REF_NO) as LOS_TD_CONTRACT_REF_NO,
    TRIM(td.LOS_CONTRACT_CODE) as LOS_CONTRACT_CODE,
    TRIM(td.MDVAY_NKT) as MDVAY_NKT,
    TRIM(td.MDVAY_SP) as MDVAY_SP,
    {{ t24_date('td.MOV_DATE') }} as LOS_TD_MOV_DATE,
    TRIM(td.TT_PP_DE_XUAT_GN) as TT_PP_DE_XUAT_GN,
    TRIM(udf_h.CONTRACT_REF_NO) as UDFH_CONTRACT_REF_NO,
    TRIM(udf_h.CT_KHUYEN_MAI_CHO_VAY) as CT_KHUYEN_MAI_CHO_VAY,
    TRIM(udf_h.CTKM2) as CTKM2,
    {{ t24_date('udf_h.MOV_DATE') }} as UDFH_MOV_DATE,
    TRIM(rt_h.CO_CAU_NO) as CO_CAU_NO,
    TRIM(rt_h.CONTRACT_REF_NO) as RTH_CONTRACT_REF_NO,
    {{ t24_date('rt_h.NGAY_HET_COVID19') }} as NGAY_HET_COVID19,
    TRIM(fac.ID) as FAC_ID,
    TRIM(fac.PRODUCT_ID) as PRODUCT_ID,
    TRIM(ld_los.AMND_STATE) as AMND_STATE,
    {{ t24_date('ld_los.BOOKING_DATE') }} as LOS_DSBR_BOOKING_DATE,
    TRIM(ld_los.EXT_REF_NO_1) as EXT_REF_NO_1,
    TRIM(ld_los.FACILITY_ID) as FACILITY_ID,
    TRIM(ld_los.STATUS) as STATUS,
    TRIM(pd_los.ID) as PROD_ID,
    TRIM(pd_los.PRODUCT_CODE) as PRODUCT_CODE,
    TRIM(pd_los.PRODUCT_NAME) as PRODUCT_NAME,
    TRIM(stmt.ACCOUNT) as ACCOUNT,
    TRIM(stmt.ACCOUNT_ID) as ACCOUNT_ID,
    TRIM(stmt.ACCOUNT_NUM) as ACCOUNT_NUM,
    TRIM(stmt.ACCOUNT_NUMBER) as ACCOUNT_NUMBER,
    {{ t24_date('stmt.ACCOUNTING_DATE') }} as ACCOUNTING_DATE,
    TRIM(stmt.ACCT_ID) as ACCT_ID,
    COALESCE(CAST(stmt.AMOUNT_FCY AS DECIMAL(25,5)), 0) as AMOUNT_FCY,
    COALESCE(CAST(stmt.AMOUNT_LCY AS DECIMAL(25,5)), 0) as AMOUNT_LCY,
    TRIM(stmt.BALANCE_TYPE) as BALANCE_TYPE,
    {{ t24_date('stmt.BOOKING_DATE') }} as BOOKING_DATE,
    TRIM(stmt.CARD_NUMBER) as CARD_NUMBER,
    TRIM(stmt.CO_CODE) as CO_CODE,
    TRIM(stmt.COMMIT_ACTION) as COMMIT_ACTION,
    stmt.COMMIT_TS as COMMIT_TS,
    TRIM(stmt.COMPANY_CODE) as COMPANY_CODE,
    TRIM(stmt.CONSOL_KEY) as CONSOL_KEY,
    TRIM(stmt.CONTRACT_BAL_ID) as CONTRACT_BAL_ID,
    TRIM(stmt.CRF_PROD_CAT) as CRF_PROD_CAT,
    TRIM(stmt.CRF_TYPE) as CRF_TYPE,
    COALESCE(CAST(stmt.CURR_NO AS DECIMAL(10)), 0) as CURR_NO,
    TRIM(stmt.CURRENCY) as CURRENCY,
    TRIM(stmt.CUSTOMER_ID) as CUSTOMER_ID,
    {{ t24_date('stmt.CYCLE_FORWARD') }} as CYCLE_FORWARD,
    {{ t24_date('stmt.DATE_TIME') }} as DATE_TIME,
    TRIM(stmt.DEPARTMENT_CODE) as DEPARTMENT_CODE,
    TRIM(stmt.DRAWN_ACCOUNT) as DRAWN_ACCOUNT,
    COALESCE(CAST(stmt.EXCHANGE_RATE AS DECIMAL(13,7)), 0) as EXCHANGE_RATE,
    TRIM(stmt.INTERNAL_CATEGORY) as INTERNAL_CATEGORY,
    TRIM(stmt.LOCAL_REF) as LOCAL_REF,
    {{ etl_date() }} as ETL_DATE
from stmt
left join m       on stmt.OUR_REFERENCE = m.CONTRACT_REF_NO
left join td      on stmt.OUR_REFERENCE = td.CONTRACT_REF_NO
left join udf     on m.CONTRACT_REF_NO = udf.CONTRACT_REF_NO
left join udf_h   on m.CONTRACT_REF_NO = udf_h.CONTRACT_REF_NO
left join rt_h    on m.CONTRACT_REF_NO = rt_h.CONTRACT_REF_NO
left join col_udf on m.CONTRACT_REF_NO = col_udf.COL_REF
left join ld_los  on stmt.OUR_REFERENCE = ld_los.EXT_REF_NO_1
left join fac     on ld_los.FACILITY_ID = fac.ID
left join pd_los  on fac.PRODUCT_ID = pd_los.ID
