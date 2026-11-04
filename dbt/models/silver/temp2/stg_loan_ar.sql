-- [STAGING] Wave 2 | PG_SILVER.LOAN_AR (31 cột) – SCD TYPE 2, 3 nhánh UNION ALL | Sheet 'Loan Arrangement' | Track 2 – Fresher 2
-- Nhánh 1: LD hiện hành | Nhánh 2: LD_HIS (đã tất toán) | Nhánh 3: PD_PAYMENT_DUE (quá hạn, AR_CODE = SUBSTR(RECID,1,14))
-- FRST_OU_CODE: theo ghi chú BA, tính FIRST_VALUE(CO_CODE) trên tập hợp nhất LD ∪ LD_HIS
--               (ORDER BY VALUE_DATE, CURR_NO) để lấy chi nhánh mở đầu tiên xuyên suốt lịch sử HĐ.
-- LMT_ID: chỉ băm khi LIMIT_REFERENCE IS NOT NULL.
-- Bảng trung gian: kết quả biến đổi (ảnh nguồn ${ETL_DATE}) của LOAN_AR được vật lý hoá trước,
-- để bước MERGE SCD2 không phải inline lại toàn bộ kế hoạch nhiều nhánh/JOIN (tránh Spark driver OOM).
{{ config(materialized='table', schema='pg_silver_stg', location_root='s3://silver/_stg', alias='stg_loan_ar', persist_docs={'relation': false, 'columns': false}) }}


with ld as (select * from {{ bz('t24core_ld_loans_and_deposits') }}),
ldh as {{ ldh_latest() }},
first_ou as (
    select AR_CODE, CO_CODE as FRST_OU_CODE from (
        select AR_CODE, CO_CODE, row_number() over (partition by AR_CODE order by VAL_D asc, CURR_NO asc) as rn
        from (
            select RECID as AR_CODE, CO_CODE, {{ t24_date('VALUE_DATE') }} as VAL_D, CURR_NO from ld
            union all
            select substr(RECID, 1, instr(RECID, ';') - 1), CO_CODE, {{ t24_date('VALUE_DATE') }}, CURR_NO
            from {{ bz('t24core_ld_loans_and_deposits_his') }}
        ) u
    ) where rn = 1
),
m as {{ latest_by(bz('flexbo_pgb_ldtb_contract_master'), 'z.CONTRACT_REF_NO', 'z.VERSION_NO desc') }},
td as {{ tdate() }},
ld_los as (
    select * from {{ bz('los_app_loan_disbursement') }} where AMND_STATE = 'F' and STATUS in ('A', 'U')
),
los as (select * from {{ bz('los_app_facility') }}),
pd_los as (select * from {{ bz('los_app_product') }}),
i as (select * from {{ bz('t24core_customer') }}),
d1 as (
    select RECID, max(ACC_FCC) as ACC_FCC, max(APPROVE_AMOUNT) as APPROVE_AMOUNT, max(CATEGORY) as CATEGORY,
           max(CO_CODE) as CO_CODE, max(FINAL_DUE_DATE) as FINAL_DUE_DATE, max(ORIGINAL_MAT) as ORIGINAL_MAT
    from {{ bz('t24core_pd_payment_due_mv') }}
    where nvl(FLAG_STATUS, '_') <> 'DELETE'
    group by RECID
),
b1 as (
    select
        'T24_LD_LOANS_AND_DEPOSITS'                                  as SRC_STM_CODE,
        {{ fn_hash('T24_LD_LOANS_AND_DEPOSITS', 'ld.RECID') }}       as AR_ID,
        cast(ld.RECID as string)                                     as AR_CODE,
        cast(ld.ACC_FCC as string)                                   as FCC_AR_CODE,
        cast(ld.APPROVE_AMOUNT as decimal(25,5))                     as ORIG_AMT_FCY,
        cast(ld.AUTHORISER as string)                                as CHKER_OFCR_CODE,
        cast(ld.CATEGORY as string)                                  as PD_CGY_CODE,
        cast(fo.FRST_OU_CODE as string)                              as FRST_OU_CODE,
        cast(ld.CRA_LD as string)                                    as PPR_CTR_NBR,
        cast(ld.CURRENCY as string)                                  as CCY_CODE,
        cast(ld.CUSTOMER_ID as string)                               as CST_CODE,
        {{ fn_hash('T24_CUSTOMER', 'ld.CUSTOMER_ID') }}              as CST_ID,
        cast(ld.DRAWDOWN_NET_AMT as decimal(25,5))                   as DSBR_AMT_FCY,
        cast(ld.EXCG_RATE as decimal(25,7))                          as EXG_RATE_TO_LCL_CCY,
        cast(ld.FCC_LIMIT as string)                                 as FCC_LMT_CODE,
        {{ t24_date('ld.FIN_MAT_DATE') }}                            as MAT_DT,
        cast(ld.INPUTTER as string)                                  as MAKER_OFCR_CODE,
        cast(ld.LIMIT_REFERENCE as string)                           as LMT_CODE,
        case when ld.LIMIT_REFERENCE is not null then {{ fn_hash('T24_LIMIT', 'ld.LIMIT_REFERENCE') }} end as LMT_ID,
        cast(ld.LOC_TERM as string)                                  as TERM_CODE,
        cast(ld.LOS_FAC_CODE as string)                              as LOS_CTR_NBR,
        cast(ld.MB_RM_BANCHEO as string)                             as REFRRER_OFCR_CODE,
        {{ t24_date('ld.ORIG_VAL_DATE') }}                           as VAL_DT,
        cast(case
                when ld.ACC_FCC is not null and i.KHOI = 'INDIV' then td.SAN_PHAM_CHO_VAY_KHCN
                when ld.ACC_FCC is not null and i.KHOI <> 'INDIV' then td.SAN_PHAM_CHO_VAY_KHDN
                else nvl(pd_los.PRODUCT_CODE, ld.PRODUCTGR_CODE)
             end as string)                                          as PD_CODE,
        cast(nvl(td.MDVAY_SP, ld.USE_OF_LOAN) as string)             as AR_PPS_TP_CODE,
        cast(ld.USE_OF_LOAN_CHA as string)                           as AR_PPS_GRP_CODE,
        {{ t24_date('ld.VALUE_DATE') }}                              as EFF_DT,
        {{ t24_date('m.BOOKING_DATE') }}                             as FCC_VAL_DT
    from ld
    left join first_ou fo on fo.AR_CODE = ld.RECID
    left join m on ld.ACC_FCC = m.CONTRACT_REF_NO
    left join td on ld.RECID = td.CONTRACT_REF_NO
                and td.MOV_DT = {{ t24_date('coalesce(ld.ORIG_VAL_DATE, ld.VALUE_DATE)') }}
    left join ld_los on ld_los.EXT_REF_NO_1 = ld.RECID
                    and {{ t24_date('ld_los.BOOKING_DATE') }} = {{ t24_date('coalesce(ld.ORIG_VAL_DATE, ld.VALUE_DATE)') }}
    left join los on ld_los.FACILITY_ID = los.ID
    left join pd_los on los.PRODUCT_ID = pd_los.ID
    left join i on i.RECID = ld.CUSTOMER_ID
),
b2 as (
    select
        'T24_LD_LOANS_AND_DEPOSITS_HIS',
        {{ fn_hash('T24_LD_LOANS_AND_DEPOSITS', 'ldh.HIS_AR_CODE') }},
        ldh.HIS_AR_CODE,
        cast(ldh.ACC_FCC as string),
        cast(ldh.APPROVE_AMOUNT as decimal(25,5)),
        cast(null as string),
        cast(ldh.CATEGORY as string),
        cast(fo.FRST_OU_CODE as string),
        cast(ldh.CRA_LD as string),
        cast(ldh.CURRENCY as string),
        cast(ldh.CUSTOMER_ID as string),
        {{ fn_hash('T24_CUSTOMER', 'ldh.CUSTOMER_ID') }},
        cast(ldh.DRAWDOWN_NET_AMT as decimal(25,5)),
        cast(ldh.EXCG_RATE as decimal(25,7)),
        cast(null as string),
        {{ t24_date('ldh.FIN_MAT_DATE') }},
        cast(null as string),
        cast(null as string),
        cast(null as string),
        cast(ldh.LOC_TERM as string),
        cast(null as string),
        cast(ldh.MB_RM_BANCHEO as string),
        {{ t24_date('ldh.ORIG_VAL_DATE') }},
        cast(case
                when ldh.ACC_FCC is not null and i.KHOI = 'INDIV' then td.SAN_PHAM_CHO_VAY_KHCN
                when ldh.ACC_FCC is not null and i.KHOI <> 'INDIV' then td.SAN_PHAM_CHO_VAY_KHDN
                else pd_los.PRODUCT_CODE
             end as string),
        cast(nvl(td.MDVAY_SP, ldh.USE_OF_LOAN) as string),
        cast(ldh.USE_OF_LOAN_CHA as string),
        {{ t24_date('ldh.VALUE_DATE') }},
        {{ t24_date('m1.BOOKING_DATE') }}
    from ldh
    left join first_ou fo on fo.AR_CODE = ldh.HIS_AR_CODE
    left join m m1 on ldh.ACC_FCC = m1.CONTRACT_REF_NO
    left join td on ldh.HIS_AR_CODE = td.CONTRACT_REF_NO
                and td.MOV_DT = {{ t24_date('coalesce(ldh.ORIG_VAL_DATE, ldh.VALUE_DATE)') }}
    left join ld_los on ld_los.EXT_REF_NO_1 = ldh.HIS_AR_CODE
                    and {{ t24_date('ld_los.BOOKING_DATE') }} = {{ t24_date('coalesce(ldh.ORIG_VAL_DATE, ldh.VALUE_DATE)') }}
    left join los on ld_los.FACILITY_ID = los.ID
    left join pd_los on los.PRODUCT_ID = pd_los.ID
    left join i on i.RECID = ldh.CUSTOMER_ID
),
b3 as (
    select
        'T24_PD_PAYMENT_DUE',
        {{ fn_hash('T24_LD_LOANS_AND_DEPOSITS', 'substr(d.RECID, 1, 14)') }},
        substr(d.RECID, 1, 14),
        cast(d1.ACC_FCC as string),
        cast(d1.APPROVE_AMOUNT as decimal(25,5)),
        cast(d.AUTHORISER as string),
        cast(d1.CATEGORY as string),
        cast(d1.CO_CODE as string),
        cast(d.CRA_LD as string),
        cast(null as string),
        cast(d.CUSTOMER as string),
        {{ fn_hash('T24_CUSTOMER', 'd.CUSTOMER') }},
        cast(null as decimal(25,5)),
        cast(null as decimal(25,7)),
        cast(null as string),
        {{ t24_date('d1.FINAL_DUE_DATE') }},
        cast(d.INPUTTER as string),
        cast(d.LIMIT_REFERENCE as string),
        case when d.LIMIT_REFERENCE is not null then {{ fn_hash('T24_LIMIT', 'd.LIMIT_REFERENCE') }} end,
        cast(null as string),
        cast(d.LOS_FAC_CODE as string),
        cast(null as string),
        {{ t24_date('coalesce(d.ORIGINAL_MAT, d1.ORIGINAL_MAT)') }},
        cast(null as string),
        cast(d.USE_OF_LOAN as string),
        cast(d.USE_OF_LOAN_CHA as string),
        {{ t24_date('d.PAYMENT_DTE_DUE') }},
        cast(null as date)
    from {{ bz('t24core_pd_payment_due') }} d
    left join d1 on d.RECID = d1.RECID
    where nvl(d.FLAG_STATUS, '_') <> 'DELETE'
),
src as (
    select * from b1
    union all select * from b2
    union all select * from b3
)
select * from src
