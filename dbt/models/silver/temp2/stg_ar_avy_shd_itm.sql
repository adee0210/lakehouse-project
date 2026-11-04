-- [STAGING] Wave 2 | PG_SILVER.AR_AVY_SHD_ITM (19 cột) – SCD TYPE 2 | Sheet 'Arrangement Activity Schedule I' | Track 4 – Fresher 4
-- Dòng lịch thanh toán bóc từ bút toán tương lai STMT_ENTRY: RECID LIKE 'F%' AND nvl(COMMIT_ACTION, '_') <> 'D' AND BOOKING_DATE = ${ETL_DATE}
-- contract_id: LD% -> OUR_REFERENCE ; PD% -> SUBSTR(OUR_REFERENCE, 3)
-- pdp: kỳ quá hạn gần nhất = PAYMENT_DTE_DUE = MAX(PREV_SCHED_DATE <= ${ETL_DATE}) – khử trùng 1 dòng / CONTRACT_ID.
-- Bảng trung gian: kết quả biến đổi (ảnh nguồn ${ETL_DATE}) của AR_AVY_SHD_ITM được vật lý hoá trước,
-- để bước MERGE SCD2 không phải inline lại toàn bộ kế hoạch nhiều nhánh/JOIN (tránh Spark driver OOM).
{{ config(materialized='table', schema='pg_silver_stg', location_root='s3://silver/_stg', alias='stg_ar_avy_shd_itm', persist_docs={'relation': false, 'columns': false}) }}


with stmt as (
    select
        case when s.OUR_REFERENCE like 'LD%' then s.OUR_REFERENCE
             when s.OUR_REFERENCE like 'PD%' then substr(s.OUR_REFERENCE, 3) end as contract_id,
        abs(coalesce(s.AMOUNT_LCY, 0)) as amount_lcy_abs,
        s.*
    from {{ bz('t24core_stmt_entry') }} s
    where s.RECID like 'F%' and nvl(s.COMMIT_ACTION, '_') <> 'D' and {{ t24_date('s.BOOKING_DATE') }} = {{ etl_date() }}
),
ld as (
    select RECID, CATEGORY, CO_CODE from {{ bz('t24core_ld_loans_and_deposits') }}
    where {{ t24_date('DATE_TIME') }} <= {{ etl_date() }}
),
sched as (
    select CONTRACT_ID, max({{ t24_date('PREV_SCHED_DATE') }}) as LAST_SCHED_DT
    from {{ bz('t24core_ld_schedule_define_mv') }}
    where {{ t24_date('PREV_SCHED_DATE') }} <= {{ etl_date() }}
    group by CONTRACT_ID
),
pdp as (
    select * from (
        select substr(pdmv.RECID, 3) as CONTRACT_ID, pdmv.PAYMENT_DTE_DUE, pdmv.CATEGORY, pdmv.CO_CODE,
               row_number() over (partition by substr(pdmv.RECID, 3) order by pdmv.PAY_TYPE desc) as rn
        from {{ bz('t24core_pd_payment_due_mv') }} pdmv
        join sched on sched.CONTRACT_ID = substr(pdmv.RECID, 3)
                  and {{ t24_date('pdmv.PAYMENT_DTE_DUE') }} = sched.LAST_SCHED_DT
        where nvl(pdmv.FLAG_STATUS, '_') <> 'DELETE'
    ) where rn = 1
),
src as (
    select
        {{ fn_hash('T24_LD_LOANS_AND_DEPOSITS', 'stmt.RECID') }}       as AR_AVY_SHD_ID,
        cast(stmt.RECID as string)                                      as AR_AVY_SHD_CODE,
        'LOAN_PD_SHD'                                                   as AR_AVY_SHD_TP,
        {{ fn_hash('T24_LD_LOANS_AND_DEPOSITS', 'stmt.contract_id') }}  as AR_ID,
        cast(stmt.contract_id as string)                                as AR_CODE,
        case
            when stmt.TRANSACTION_CODE = '402' then 'LD_GN'
            when stmt.TRANSACTION_CODE in ('420', '423') and coalesce(ld.CATEGORY, pdp.CATEGORY, stmt.CRF_PROD_CAT, '_') <> '21069' then 'LD_PR'
            when stmt.TRANSACTION_CODE in ('434', '424')
              or (stmt.TRANSACTION_CODE in ('420', '423') and coalesce(ld.CATEGORY, pdp.CATEGORY, stmt.CRF_PROD_CAT) = '21069') then 'LD_IN'
            when stmt.TRANSACTION_CODE = '750' then 'PD_PR'
            when stmt.TRANSACTION_CODE = '751' then 'PD_IN'
            when stmt.TRANSACTION_CODE = '752' then 'PD_PE'
            when stmt.TRANSACTION_CODE = '753' then 'PD_PS'
        end                                                             as PBL_AMT_TP,
        cast(abs(coalesce(stmt.AMOUNT_LCY, 0)) as decimal(25,5))        as PBL_AMT_LCY,
        cast(abs(coalesce(stmt.AMOUNT_FCY, 0)) as decimal(25,5))        as PBL_AMT_FCY,
        cast(stmt.EXCHANGE_RATE as decimal(25,7))                       as EXG_RATE,
        cast(stmt.CURRENCY as string)                                   as CCY_CODE,
        {{ t24_date('stmt.VALUE_DATE') }}                               as EFF_MAT_DT,
        cast(case when pdp.CONTRACT_ID is not null then 1 else 0 end as decimal(1,0)) as F_LAST_OD_DT,
        {{ t24_date('pdp.PAYMENT_DTE_DUE') }}                           as LAST_OD_DT,
        'T24CORE_STMT_ENTRY'                                            as SRC_STM_CODE,
        {{ fn_hash('T24_CUSTOMER', 'stmt.CUSTOMER_ID') }}               as CST_ID,
        cast(stmt.CUSTOMER_ID as string)                                as CST_CODE
    from stmt
    left join ld  on ld.RECID = stmt.contract_id
    left join pdp on stmt.contract_id = pdp.CONTRACT_ID
)
select * from src
