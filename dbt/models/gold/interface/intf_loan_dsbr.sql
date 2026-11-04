-- Wave 4 (02:30) | PG_GOLD.INTF_LOAN_DSBR (24 cột) – SNAPSHOT (CDR_DT) | Sheet 'Interface Loan Disbursement' | Track 4 – Fresher 4
-- LOAN_AR a -> LOAN_AR_PRFL b, AR_X_IP_RLTNP c, EMPE d (RM giới thiệu), AR_RATE_HIST e, PD f – tất cả SYS_EXP_DT IS NULL.
{{ config(materialized='incremental', incremental_strategy='append', partition_by=['CDR_DT'],
          pre_hook="{{ delete_partition('CDR_DT') }}", alias='intf_loan_dsbr') }}

select
    {{ etl_date() }}                                                            as CDR_DT,
    a.AR_CODE,
    a.CST_CODE,
    b.OU_CODE,
    coalesce(a.VAL_DT, a.EFF_DT)                                                as VAL_DT,
    a.EFF_DT,
    a.MAT_DT,
    a.CCY_CODE,
    a.AR_PPS_TP_CODE,
    a.PPR_CTR_NBR,
    a.PD_CGY_CODE,
    a.ORIG_AMT_FCY,
    cast(decode(a.CCY_CODE, 'VND', 1, a.EXG_RATE_TO_LCL_CCY) as decimal(25,7))  as EXG_RATE_TO_LCL_CCY,
    cast(a.ORIG_AMT_FCY * decode(a.CCY_CODE, 'VND', 1, a.EXG_RATE_TO_LCL_CCY) as decimal(25,5)) as ORIG_AMT_LCY,
    d.EMPE_NM,
    e.EFF_RATE_PCT,
    b.AR_PPS_BY_IDY,
    b.LOS_CTR_CODE,
    b.CAR_AP_CODE,
    b.DSBR_PRPSL_OFCR,
    b.DSBR_PRPSL_MGR,
    a.PD_CODE,
    f.PD_NM,
    a.TERM_CODE                                                                 as LOC_TERM
from {{ ref('loan_ar') }} a
left join {{ ref('loan_ar_prfl') }} b  on a.AR_CODE = b.AR_CODE and b.SYS_EXP_DT is null
left join {{ ref('ar_x_ip_rltnp') }} c on a.AR_CODE = c.AR_CODE and c.SYS_EXP_DT is null
left join {{ ref('empe') }} d          on coalesce(c.REFRRER_OFCR, b.REFRRER_OFCR) = d.EMPE_CODE and d.SYS_EXP_DT is null
left join {{ ref('ar_rate_hist') }} e  on a.AR_CODE = e.AR_CODE and e.SYS_EXP_DT is null
left join {{ ref('pd') }} f            on a.PD_CODE = f.PD_CODE and f.SYS_EXP_DT is null
where a.SYS_EXP_DT is null
