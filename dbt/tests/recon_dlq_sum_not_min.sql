-- AR_DLQ_SMY.PNP_ARS phải = SUM (không phải MIN) dư nợ gốc quá hạn của các dòng PR hợp lệ
with a as (
    select substr(RECID, 1, 14) as AR_CODE, sum(PAY_AMT_OUTS) as v from (
        select h.*, max(DATE_TIME) over (partition by RECID, PAY_TYPE) mdt, max(CURR_NO) over (partition by RECID, PAY_TYPE) mcn
        from {{ ref('t24core_pd_payment_due_his_mv') }} h where ETL_DATE = {{ etl_date() }}
    ) where DATE_TIME = mdt and CURR_NO = mcn and FLAG_STATUS = 'LIVE' and PAY_TYPE = 'PR'
          and cast(CATEGORY as int) <> 21069 and RECORD_STATUS is null and PAY_AMT_OUTS > 0 and nvl(PD_TYPE, '0') <> '1'
    group by substr(RECID, 1, 14)
)
select a.AR_CODE, a.v, d.PNP_ARS from a join {{ ref('ar_dlq_smy') }} d on a.AR_CODE = d.AR_CODE and d.CDR_DT = {{ etl_date() }}
where abs(a.v - d.PNP_ARS) > 0.01
