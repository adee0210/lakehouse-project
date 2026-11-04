-- [STAGING] Wave 2 | PG_SILVER.LOAN_AR_PRFL (37 cột) – SCD TYPE 2, 3 nhánh UNION ALL | Sheet 'Loan Arrangement Profile' | Track 2
-- 37 cột đồng bộ 100% thứ tự & kiểu dữ liệu giữa 3 nhánh.
-- Chống fan-out: bảng phụ (b,e,f,g,h) lấy 1 dòng / khoá join; h = DISTINCT TRANS_REF, SBV_CODE LIKE '39%'.
-- INT_LQD_AR_ID / PNP_LQD_AR_ID / GL_ITM_ID: chỉ băm khi cột nguồn IS NOT NULL.
-- Bảng trung gian: kết quả biến đổi (ảnh nguồn ${ETL_DATE}) của LOAN_AR_PRFL được vật lý hoá trước,
-- để bước MERGE SCD2 không phải inline lại toàn bộ kế hoạch nhiều nhánh/JOIN (tránh Spark driver OOM).
{{ config(materialized='table', schema='pg_silver_stg', location_root='s3://silver/_stg', alias='stg_loan_ar_prfl', persist_docs={'relation': false, 'columns': false}) }}


with ldh as {{ ldh_latest() }},
b_udf as (
    select COL_REF, FUNCTION_CODE, max(UDF_VALUE) as UDF_VALUE
    from {{ bz('ebanking_col_udf_value') }} where UDF_ID = 'NHANNO_BATBUOC'
    group by COL_REF, FUNCTION_CODE
),
c_map as (
    select CONTRACT_REF_NO, upper(FIELD_NAME) as FIELD_NAME, max(FIELD_VAL) as FIELD_VAL
    from {{ bz('flexbo_pgb_contract_udf_map') }} group by CONTRACT_REF_NO, upper(FIELD_NAME)
),
td as {{ tdate() }},
e_hist as {{ latest_by(bz('flexbo_pgbld_contract_udfield_hist'), 'z.CONTRACT_REF_NO', 'z.MOV_DATE desc') }},
f_mg   as {{ latest_by(bz('t24core_mb_mg_saving_multi'), 'z.LD_ID', 'z.MB_LD_TYPE') }},
g_rt   as {{ latest_by(bz('flexbo_pgbld_rt_contract_udfield_hist'), 'z.CONTRACT_REF_NO', 'z.NGAY_HET_COVID19 desc') }},
h_mv   as (
    select TRANS_REF, max(SBV_CODE) as SBV_CODE
    from (select distinct TRANS_REF, SBV_CODE from {{ bz('source_saoke_mvmt') }} where SBV_CODE like '39%')
    group by TRANS_REF
),
i as (select * from {{ bz('t24core_customer') }}),
d1 as (
    select RECID, max(CO_CODE) as CO_CODE, max(REPAYMENT_ACCT) as REPAYMENT_ACCT
    from {{ bz('t24core_pd_payment_due_mv') }} where nvl(FLAG_STATUS, '_') <> 'DELETE' group by RECID
),
b1 as (
    select
        'T24_LD_LOANS_AND_DEPOSITS'                                      as SRC_STM_CODE,
        {{ fn_hash('T24_LD_LOANS_AND_DEPOSITS', 'a.RECID') }}            as AR_ID,
        cast(a.RECID as string)                                          as AR_CODE,
        cast(a.INT_LIQ_ACCT as string)                                   as INT_LQD_AR_CODE,
        case when a.INT_LIQ_ACCT is not null then {{ fn_hash('T24_LD_LOANS_AND_DEPOSITS', 'a.INT_LIQ_ACCT') }} end as INT_LQD_AR_ID,
        cast(a.PRIN_LIQ_ACCT as string)                                  as PNP_LQD_AR_CODE,
        case when a.PRIN_LIQ_ACCT is not null then {{ fn_hash('T24_LD_LOANS_AND_DEPOSITS', 'a.PRIN_LIQ_ACCT') }} end as PNP_LQD_AR_ID,
        cast(a.STATUS as string)                                         as AR_LCS_TP_CODE,
        cast(a.CO_CODE as string)                                        as OU_CODE,
        cast(coalesce(b.UDF_VALUE, bl.UDF_VALUE) as string)              as MAND_REPYMT,
        cast(nvl(d.SO_LAN_CO_CAU, c.FIELD_VAL) as decimal(22,0))         as NBR_OF_DBT_RSTC,
        cast(c1.FIELD_VAL as string)                                     as SCR_TP,
        cast(d.CAR_APPLICATION_CODE as string)                           as CAR_AP_CODE,
        cast(d.LOS_CONTRACT_CODE as string)                              as LOS_CTR_CODE,
        cast(d.LOS_CONTRACT_ID as string)                                as LOS_CTR_ID,
        cast(d.CO_CAU_NO_MIEN_GIAM_LAI as string)                        as DBT_WVR_F,
        cast(d.MDVAY_NKT as string)                                      as AR_PPS_BY_IDY,
        cast(d.MDVAY_SP as string)                                       as AR_PPS_BY_PD,
        cast(d.CAN_BO_DE_XUAT_GN as string)                              as DSBR_PRPSL_OFCR,
        cast(d.TT_PP_DE_XUAT_GN as string)                               as DSBR_PRPSL_MGR,
        cast(d.CBTD_QL_KHOAN_VAY as string)                              as MGT_OFCR,
        cast(d.TP_PP_QL_KHOAN_VAY as string)                             as MGT_MGR,
        cast(d.CAP_PHE_DUYET_GNTL as string)                             as APRV_AHR,
        cast(a.AUTHORISER as string)                                     as CHKER_OFCR,
        cast(a.INPUTTER as string)                                       as MAKER_OFCR,
        cast(a.MB_RM_BANCHEO as string)                                  as REFRRER_OFCR,
        cast(case when e.CT_KHUYEN_MAI_CHO_VAY is null and i.KHOI = 'INDIV' then f.MB_LD_TYPE else e.CT_KHUYEN_MAI_CHO_VAY end as string) as PROM_PRGM,
        cast(case when e.CTKM2 is null and i.KHOI <> 'INDIV' then f.MB_LD_TYPE else e.CTKM2 end as string) as PROM_PRGM_2,
        cast(g.CO_CAU_NO as string)                                      as DBT_RSTC_TP_CODE,
        {{ t24_date('g.NGAY_HET_COVID19') }}                             as COVID19_RSTC_COMPL_DT,
        cast(h.SBV_CODE as string)                                       as GL_ITM_CODE,
        case when h.SBV_CODE is not null then {{ fn_hash('T24_GL_ITM', 'h.SBV_CODE') }} end as GL_ITM_ID,
        '1'                                                              as FNC_ST_CODE,
        cast(d.BDS_TT11 as string)                                       as BDS_TT11
    from {{ bz('t24core_ld_loans_and_deposits') }} a
    -- b: (COL_REF = ACC_FCC & CONTRACT_INFOR & RECID LIKE 'LD%') OR (COL_REF = FCC_LIMIT & LIMIT_INFOR)
    left join b_udf b  on b.COL_REF = a.ACC_FCC and b.FUNCTION_CODE = 'CONTRACT_INFOR' and a.RECID like 'LD%'
    left join b_udf bl on bl.COL_REF = a.FCC_LIMIT and bl.FUNCTION_CODE = 'LIMIT_INFOR'
    left join c_map c  on c.CONTRACT_REF_NO = a.ACC_FCC and c.FIELD_NAME = 'SO LAN CO CAU'
    left join c_map c1 on c1.CONTRACT_REF_NO = a.ACC_FCC and c1.FIELD_NAME = 'LOAI CHUNG KHOAN'
    left join td d     on d.CONTRACT_REF_NO = a.RECID and d.MOV_DT = {{ t24_date('coalesce(a.ORIG_VAL_DATE, a.VALUE_DATE)') }}
    left join e_hist e on e.CONTRACT_REF_NO = a.ACC_FCC
    left join f_mg f   on f.LD_ID = a.RECID
    left join g_rt g   on g.CONTRACT_REF_NO = a.ACC_FCC
    left join h_mv h   on h.TRANS_REF = a.RECID
    left join i        on i.RECID = a.CUSTOMER_ID
),
b2 as (
    select
        'T24_LD_LOANS_AND_DEPOSITS_HIS',
        {{ fn_hash('T24_LD_LOANS_AND_DEPOSITS', 'a.HIS_AR_CODE') }},
        a.HIS_AR_CODE,
        cast(a.INT_LIQ_ACCT as string),
        case when a.INT_LIQ_ACCT is not null then {{ fn_hash('T24_LD_LOANS_AND_DEPOSITS', 'a.INT_LIQ_ACCT') }} end,
        cast(a.PRIN_LIQ_ACCT as string),
        case when a.PRIN_LIQ_ACCT is not null then {{ fn_hash('T24_LD_LOANS_AND_DEPOSITS', 'a.PRIN_LIQ_ACCT') }} end,
        cast(null as string),
        cast(a.CO_CODE as string),
        cast(b.UDF_VALUE as string),
        cast(nvl(d.SO_LAN_CO_CAU, c.FIELD_VAL) as decimal(22,0)),
        cast(c1.FIELD_VAL as string),
        cast(d.CAR_APPLICATION_CODE as string),
        cast(d.LOS_CONTRACT_CODE as string),
        cast(d.LOS_CONTRACT_ID as string),
        cast(d.CO_CAU_NO_MIEN_GIAM_LAI as string),
        cast(d.MDVAY_NKT as string),
        cast(d.MDVAY_SP as string),
        cast(d.CAN_BO_DE_XUAT_GN as string),
        cast(d.TT_PP_DE_XUAT_GN as string),
        cast(d.CBTD_QL_KHOAN_VAY as string),
        cast(d.TP_PP_QL_KHOAN_VAY as string),
        cast(d.CAP_PHE_DUYET_GNTL as string),
        cast(null as string),
        cast(null as string),
        cast(a.MB_RM_BANCHEO as string),
        cast(case when e.CT_KHUYEN_MAI_CHO_VAY is null and i.KHOI = 'INDIV' then f.MB_LD_TYPE else e.CT_KHUYEN_MAI_CHO_VAY end as string),
        cast(case when e.CTKM2 is null and i.KHOI <> 'INDIV' then f.MB_LD_TYPE else e.CTKM2 end as string),
        cast(g.CO_CAU_NO as string),
        {{ t24_date('g.NGAY_HET_COVID19') }},
        cast(h.SBV_CODE as string),
        case when h.SBV_CODE is not null then {{ fn_hash('T24_GL_ITM', 'h.SBV_CODE') }} end,
        '1',
        cast(d.BDS_TT11 as string)
    from ldh a
    left join b_udf b  on b.COL_REF = a.ACC_FCC and b.FUNCTION_CODE = 'CONTRACT_INFOR' and a.RECID like 'LD%'
    left join c_map c  on c.CONTRACT_REF_NO = a.ACC_FCC and c.FIELD_NAME = 'SO LAN CO CAU'
    left join c_map c1 on c1.CONTRACT_REF_NO = a.ACC_FCC and c1.FIELD_NAME = 'LOAI CHUNG KHOAN'
    left join td d     on d.CONTRACT_REF_NO = a.HIS_AR_CODE and d.MOV_DT = {{ t24_date('coalesce(a.ORIG_VAL_DATE, a.VALUE_DATE)') }}
    left join e_hist e on e.CONTRACT_REF_NO = a.ACC_FCC
    left join f_mg f   on f.LD_ID = a.HIS_AR_CODE
    left join g_rt g   on g.CONTRACT_REF_NO = a.ACC_FCC
    left join h_mv h   on h.TRANS_REF = a.HIS_AR_CODE
    left join i        on i.RECID = a.CUSTOMER_ID
),
b3 as (
    select
        'T24_PD_PAYMENT_DUE',
        {{ fn_hash('T24_LD_LOANS_AND_DEPOSITS', 'substr(d.RECID, 1, 14)') }},
        substr(d.RECID, 1, 14),
        cast(coalesce(d.REPAYMENT_ACCT, d1.REPAYMENT_ACCT) as string),
        case when coalesce(d.REPAYMENT_ACCT, d1.REPAYMENT_ACCT) is not null
             then {{ fn_hash('T24_LD_LOANS_AND_DEPOSITS', 'coalesce(d.REPAYMENT_ACCT, d1.REPAYMENT_ACCT)') }} end,
        cast(null as string), cast(null as string),
        cast(d.STATUS as string),
        cast(d1.CO_CODE as string),
        cast(null as string), cast(null as decimal(22,0)), cast(null as string), cast(null as string),
        cast(null as string), cast(null as string), cast(null as string),
        cast(null as string), cast(null as string), cast(null as string), cast(null as string),
        cast(null as string), cast(null as string), cast(null as string),
        cast(d.AUTHORISER as string),
        cast(d.INPUTTER as string),
        cast(null as string), cast(null as string), cast(null as string), cast(null as string),
        cast(null as date), cast(null as string), cast(null as string),
        cast(nvl(d.CREDIT_STATUS, '1') as string),
        cast(null as string)
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
