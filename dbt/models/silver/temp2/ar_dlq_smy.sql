-- Wave 2 | PG_SILVER.AR_DLQ_SMY (11 cột) – SNAPSHOT (CDR_DT) | Sheet 'Arrangement Delinquency Summary' | Track 6
-- Gom theo SUBSTR(a.RECID,1,14): ngày bắt đầu quá hạn = MIN, tiền quá hạn = SUM (không phải MIN), SNQH chuyển đổi = MAX.
-- a: bản ghi LIVE, PR/IN, PD_TYPE <> '1', chưa REVE, còn nợ > 0, DATE_TIME & CURR_NO mới nhất theo (RECID, PAY_TYPE)
{{ config(materialized='incremental', incremental_strategy='append', partition_by=['CDR_DT'],
          pre_hook="{{ delete_partition('CDR_DT') }}", alias='ar_dlq_smy') }}

with his as (
    select h.*,
           max(h.DATE_TIME) over (partition by h.RECID, h.PAY_TYPE) as _max_dt,
           max(cast(h.CURR_NO as int)) over (partition by h.RECID, h.PAY_TYPE) as _max_curr
    from {{ bz('t24core_pd_payment_due_his_mv') }} h
),
a as (
    select * from his
    where FLAG_STATUS = 'LIVE'
      and PAY_TYPE in ('PR', 'IN')
      and nvl(PD_TYPE, '0') <> '1'
      and RECORD_STATUS is null
      and nvl(RECORD_STATUS, '_') <> 'REVE'
      and cast(PAY_AMT_OUTS as decimal(25,5)) > 0
      and DATE_TIME = _max_dt
      and cast(CURR_NO as int) = _max_curr
),
a14 as (select substr(RECID, 1, 14) as AR_CODE14, * from a),
b as (select RECID, max(SNQH_CHUYENDOI) as SNQH_CHUYENDOI from {{ bz('t24core_pd_payment_due') }} group by RECID)
select
    {{ etl_date() }}                                                                  as CDR_DT,
    {{ fn_hash('T24_LD_LOANS_AND_DEPOSITS', 'a.AR_CODE14') }}                         as AR_ID,
    a.AR_CODE14                                                                       as AR_CODE,
    {{ fn_hash('T24_LD_LOANS_AND_DEPOSITS', "case when a.AR_CODE14 like 'PDLD%' then substr(a.AR_CODE14, 3, 12) else a.AR_CODE14 end") }} as ORIG_AR_ID,
    case when a.AR_CODE14 like 'PDLD%' then substr(a.AR_CODE14, 3, 12) else a.AR_CODE14 end as ORIG_AR_CODE,
    'T24_PD_PAYMENT_DUE_HIS_MV'                                                       as SRC_STM_CODE,
    min(case when a.PAY_TYPE = 'PR' and cast(a.CATEGORY as int) <> 21069
             then {{ t24_date('a.PAYMENT_DTE_DUE') }} end)                            as PNP_PAST_DUE_DT,
    min(case when a.PAY_TYPE = 'IN' or (cast(a.CATEGORY as int) = 21069 and a.PAY_TYPE = 'PR')
             then {{ t24_date('a.PAYMENT_DTE_DUE') }} end)                            as INT_PAST_DUE_DT,
    cast(sum(case when a.PAY_TYPE = 'PR' and cast(a.CATEGORY as int) <> 21069
                  then cast(a.PAY_AMT_OUTS as decimal(25,5)) else 0 end) as decimal(25,5))                   as PNP_ARS,
    cast(sum(case when a.PAY_TYPE = 'IN' or (cast(a.CATEGORY as int) = 21069 and a.PAY_TYPE = 'PR')
                  then cast(a.PAY_AMT_OUTS as decimal(25,5)) else 0 end) as decimal(25,5))                   as INT_ARS,
    cast(max(b.SNQH_CHUYENDOI) as decimal(22,0))                                      as ADDITION_DYS_IN_ARS
from a14 a
left join b on a.RECID = b.RECID
group by a.AR_CODE14
