-- Wave 4 (02:30) | PG_GOLD.INTF_LOAN_AR (88 cột) – SNAPSHOT (CDR_DT) | Sheet 'Interface Loan Arrangement' | Track 2 – Fresher 2
-- Grain = PG_SILVER.AR_BAL (AR_ID x GL_ITM_CODE) tại ${ETL_DATE}. Chạy sau INF_CST.
-- Dư nợ gốc: GL_ITM_CODE LIKE '2%' ; lãi dự thu: '394%'. Mọi bảng SCD2 lọc SYS_EXP_DT IS NULL; subquery e1/k/shd lọc CDR_DT.
-- ⚠ SỬA LỖI BA (header block):
--   (1) dòng AR_RATE_HIST alias 'd' nhưng điều kiện viết 'a.AR_ID = e.AR_ID AND e.CDR_DT = ...' (alias & cột sai,
--       AR_RATE_HIST là SCD2 không có CDR_DT) -> sửa: a.AR_ID = d.AR_ID AND d.SYS_EXP_DT IS NULL.
--   (2) AR_DLQ_SMY 'e': theo ghi chú "Join 1:1 theo AR_ID (dành cho PDLD%); HĐ gốc LD% dùng subquery e1"
--       -> e chỉ join khi a.AR_CODE LIKE 'PDLD%' (tránh fan-out: 1 HĐ LD có thể có nhiều dòng PD).
{{ config(materialized='incremental', incremental_strategy='append', partition_by=['CDR_DT'],
          pre_hook="{{ delete_partition('CDR_DT') }}", alias='intf_loan_ar') }}

{% set orig_key = "case when a.AR_CODE like 'PDLD%' then " ~ fn_hash('T24_LD_LOANS_AND_DEPOSITS', 'substr(a.AR_CODE, 3, 12)') ~ " else a.AR_ID end" %}

with a as (select * from {{ ref('ar_bal') }} where CDR_DT = {{ etl_date() }}),
e1 as (
    select
        ORIG_AR_ID,
        sum(nvl(e.PNP_ARS, 0)) as PNP_ARS,
        max(datediff({{ etl_date() }}, e.PNP_PAST_DUE_DT)) + 1 as SO_NGAY_QH_GOC,
        max(datediff({{ etl_date() }}, e.INT_PAST_DUE_DT)) + 1 as SO_NGAY_QH_LAI,
        greatest(nvl(max(datediff({{ etl_date() }}, e.PNP_PAST_DUE_DT)) + 1, 0),
                 nvl(max(datediff({{ etl_date() }}, e.INT_PAST_DUE_DT)) + 1, 0)) as SONGAY_QHAN_CAONHAT,
        max(nvl(e.ADDITION_DYS_IN_ARS, 0)) as SNQH_CHUYENDOI
    from {{ ref('ar_dlq_smy') }} e
    where e.CDR_DT = {{ etl_date() }}
    group by ORIG_AR_ID
),
k as (
    select ORIG_AR_ID,
           sum(TOT_ACR_INT_AMT_FCY) as TOT_ACR_INT_AMT_FCY, sum(TOT_INT_PAID_AMT_FCY) as TOT_INT_PAID_AMT_FCY,
           sum(TOT_INT_DUE_AMT_FCY) as TOT_INT_DUE_AMT_FCY, sum(TOT_INT_ODUE_AMT_FCY) as TOT_INT_ODUE_AMT_FCY
    from {{ ref('ast_ar_int_smy') }} where CDR_DT = {{ etl_date() }}
    group by ORIG_AR_ID
),
shd as (
    select AR_ID,
           min(case when PBL_AMT_TP in ('LD_PR', 'PD_PR') then EFF_MAT_DT end) as NGAY_THANH_TOAN_GOC_TIEPTHEO,
           sum(case when PBL_AMT_TP in ('LD_PR', 'PD_PR') then nvl(PBL_AMT_FCY, 0) else 0 end) as SO_TIEN_THUNO_GOC,
           min(case when PBL_AMT_TP in ('LD_IN', 'PD_IN') then EFF_MAT_DT end) as NGAY_THANH_TOAN_LAI
    from {{ ref('ar_avy_shd_itm') }}
    where SYS_EXP_DT is null and EFF_MAT_DT >= {{ etl_date() }}
    group by AR_ID
)
select
    {{ etl_date() }}                                          as CDR_DT,
    a.AR_CODE,
    b.EFF_DT,
    a.PD_CGY_CODE,
    b.PPR_CTR_NBR,
    c.OU_CODE,
    a.SUB_PD_CODE,
    a.CST_CODE,
    c.AR_LCS_TP_CODE,
    a.MUD_CODE,
    c.FNC_ST_CODE,
    b.CCY_CODE,
    b.MAT_DT,
    cast(nvl(b.ORIG_AMT_FCY, 0) as decimal(25,5))             as APPROVE_AMOUNT,
    coalesce(b.VAL_DT, b.FCC_VAL_DT, b.EFF_DT)                as VAL_DT,
    c.PNP_LQD_AR_CODE                                         as DRAWDOWN_ACCOUNT,
    a.AR_TERM_TP_CODE,
    b.LMT_CODE,
    cast(null as decimal(22,0))                               as INT_KEY,
    d.SPRD_RATE_PCT                                           as INT_SPRD,
    c.INT_LQD_AR_CODE,
    b.FCC_AR_CODE,
    d.PNY_RATE,
    d.ODUE_INT_RATE,
    cast(case when a.GL_ITM_CODE like '2%' and a.RCVB_AMT_LCY < 0 then abs(a.RCVB_AMT_LCY) else 0 end as decimal(25,5)) as AMOUNT_CUR,
    d.INT_RATE_VARIABLITY_TP_CODE                             as INT_TP,
    cast(null as string)                                      as INT_BSS,
    case when d.INT_RATE_VARIABLITY_TP_CODE in ('1', 'FIX', 'FIXED') then 'Y'
         when d.INT_RATE_VARIABLITY_TP_CODE is not null then 'N' end as INT_FIX,
    d.EFF_RATE_PCT                                            as RATE,
    a.CDR_DT                                                  as TXN_DT,
    cast(nvl(e.PNP_ARS, nvl(e1.PNP_ARS, 0)) as decimal(25,5)) as ODUE_PR_AMT,
    cast(nvl(k.TOT_INT_ODUE_AMT_FCY, 0) as decimal(32,5))     as ODUE_IN_AMT,
    cast(0 as decimal(25,5))                                  as ODUE_PE_AMT,
    cast(0 as decimal(25,5))                                  as ODUE_PS_AMT,
    d.MRGN_RATE,
    b.PD_CODE,
    case when cst.CST_GRP_CODE like '%INDIV%' then b.PD_CODE end as KHCN_PD_CODE,
    c.DSBR_PRPSL_OFCR,
    c.DSBR_PRPSL_MGR,
    c.MGT_OFCR,
    c.MGT_MGR,
    c.APRV_AHR,
    c.DBT_WVR_F,
    c.NBR_OF_DBT_RSTC,
    c.AR_PPS_BY_PD,
    cast(nvl(e1.SO_NGAY_QH_GOC, 0) as decimal(22,0))          as DYS_IN_PNP_ARS,
    cast(nvl(e1.SO_NGAY_QH_LAI, 0) as decimal(22,0))          as DYS_IN_INT_ARS,
    cast(nvl(k.TOT_INT_DUE_AMT_FCY, 0) + nvl(k.TOT_INT_ODUE_AMT_FCY, 0) as decimal(32,5)) as TOT_INT_ARS,
    a.CST_ID,
    cst.CST_FNC_ST_CODE,
    f.MID_RATE,
    cast(case when a.GL_ITM_CODE like '2%' and a.CLS_BAL_LCY < 0 then abs(a.CLS_BAL_LCY) else 0 end as decimal(25,5)) as AMT_LCY,
    cast(nvl(b.ORIG_AMT_FCY, 0) * decode(b.CCY_CODE, 'VND', 1, nvl(f.MID_RATE, b.EXG_RATE_TO_LCL_CCY)) as decimal(25,5)) as APRV_AMT_LCY,
    cast(nvl(e1.SONGAY_QHAN_CAONHAT, 0) + nvl(e1.SNQH_CHUYENDOI, 0) as decimal(22,0)) as MX_DYS_IN_ARS,
    cast(nvl(k.TOT_INT_PAID_AMT_FCY, 0) as decimal(32,5))     as TOT_INT_PAID_AMT_FCY,
    c.FNC_ST_CODE                                             as AR_FNC_ST_CODE,
    cst.JNT_LBY_F,
    b.FCC_LMT_CODE,
    c.MAKER_OFCR                                              as MAKER_OFCR_CODE,
    c.CHKER_OFCR                                              as CHKER_OFCR_CODE,
    cst.CST_GRP_CODE,
    case when b.FCC_AR_CODE is not null then substr(c.AR_PPS_BY_IDY, 2, length(c.AR_PPS_BY_IDY)) else b.AR_PPS_TP_CODE end as AR_PPS_TP_CODE,
    b.AR_PPS_GRP_CODE,
    c.LOS_CTR_CODE,
    c.LOS_CTR_ID,
    c.DBT_RSTC_TP_CODE,
    c.CAR_AP_CODE,
    c.SCR_TP,
    c.MAND_REPYMT,
    b.LOS_CTR_NBR,
    c.PROM_PRGM,
    c.PROM_PRGM_2,
    cst.SCTR_CODE,
    c.COVID19_RSTC_COMPL_DT,
    shd.NGAY_THANH_TOAN_GOC_TIEPTHEO,
    cast(nvl(shd.SO_TIEN_THUNO_GOC, 0) as decimal(25,5))      as SO_TIEN_THUNO_GOC,
    shd.NGAY_THANH_TOAN_LAI,
    cst.IDY_LV1_CL_CODE,
    cst.IDY_LV2_CL_CODE,
    cast(case when a.MUD_CODE = 'LD' and a.GL_ITM_CODE like '394%' then abs(a.RCVB_AMT_LCY) else 0 end as decimal(25,5)) as TOT_ACR_INT_DUE_AMT,
    cast(case when a.MUD_CODE = 'LD' and a.GL_ITM_CODE like '394%' then abs(a.RCVB_AMT_LCY) else 0 end as decimal(25,5)) as NXT_ACR_INT_AMT,
    cast(greatest(nvl(k.TOT_ACR_INT_AMT_FCY, 0) - nvl(k.TOT_INT_PAID_AMT_FCY, 0), 0) as decimal(25,5)) as DTHU_CONLAI_DUKIEN_KYGANNHAT,
    i.OU_NM,
    cast(nvl(e1.SNQH_CHUYENDOI, 0) as decimal(22,0))          as ADDITION_DYS_IN_ARS,
    c.BDS_TT11,
    a.GL_ITM_CODE,
    case when cst.CST_GRP_CODE like '%INDIV%' then 'INDIV' else 'CORP' end as CST_TP_CODE,
    cst.CST_NM
from a
join {{ ref('loan_ar') }} b           on a.AR_ID = b.AR_ID and b.SYS_EXP_DT is null
left join {{ ref('loan_ar_prfl') }} c on a.AR_ID = c.AR_ID and c.SYS_EXP_DT is null
left join {{ ref('ar_rate_hist') }} d on a.AR_ID = d.AR_ID and d.SYS_EXP_DT is null
left join {{ ref('ar_dlq_smy') }} e   on a.AR_CODE like 'PDLD%' and a.AR_ID = e.AR_ID and e.CDR_DT = {{ etl_date() }}
left join e1                          on {{ orig_key }} = e1.ORIG_AR_ID
left join {{ ref('exg_rate') }} f     on b.CCY_CODE = f.FRST_CCY_CODE and f.SNPST_DT = {{ etl_date() }}
left join {{ ref('ou') }} i           on c.OU_CODE = i.OU_CODE and i.SYS_EXP_DT is null
left join k                           on {{ orig_key }} = k.ORIG_AR_ID
left join {{ ref('inf_cst') }} cst    on a.CST_CODE = cst.CST_CODE and cst.CDR_DT = {{ etl_date() }}
left join shd                         on a.AR_ID = shd.AR_ID
