-- Wave 2 (01:45) | PG_SILVER.AR_TVR_SMY (24 cột) – Fact Snapshot (CDR_DT) | Sheet 'Arrangement Turnover Summary' | Track 6
-- Chạy SAU PST_ENTR, AR_DLQ_SMY, LOAN_AR.
-- stmt: gom trước PST_ENTR theo AR_CODE (tránh nhân bản N x M), doanh số Nợ/Có theo Ngày / MTD / YTD.
--
-- ⚠ SỬA LỖI BA: trong sheet, cột Transform của 12 dòng TOT_ODUE_PNP_AMT_FCY .. NBR_OF_DB_IN_YR bị lệch 1-2 dòng so với
--   cột đích (vd TOT_ODUE_PNP_AMT_FCY lại map NVL(stmt.DB_AMT_IN_DAY_FCY,0), DSBR_AMT_LTD_FCY map NBR_OF_DB_IN_DAY).
--   Code dưới map theo TÊN CỘT ĐÍCH + cột "Bảng nguồn/Cột nguồn" của chính dòng đó:
--     TOT_ODUE_PNP_AMT_FCY <- AR_DLQ_SMY.PNP_ARS ; DSBR_AMT_LTD_FCY <- LOAN_AR.DSBR_AMT_FCY/ORIG_AMT_FCY ;
--     *_IN_DAY/_IN_MO/_IN_YR <- đúng chỉ tiêu cùng tên trong subquery stmt.
{{ config(materialized='incremental', incremental_strategy='append', partition_by=['CDR_DT'],
          pre_hook="{{ delete_partition('CDR_DT') }}", alias='ar_tvr_smy') }}

-- Nguồn lưu số tiền dạng VARCHAR2: ép DECIMAL(25,5) tường minh (tránh Spark ép ngầm sang INT -> tràn số > 2^31 thành NULL)
with a as (
    select APP_ID, SBV_CODE, MAT_DATE, TXN_DATE,
           cast(AMOUNT_CUR as decimal(25,5)) as AMOUNT_CUR, cast(AMOUNT_LCY as decimal(25,5)) as AMOUNT_LCY, APP_CODE
    from {{ bz('source_saoke_crb') }}
    where APP_CODE in ('LD', 'PD') and {{ t24_date('TXN_DATE') }} = {{ etl_date() }}
),
stmt as (
    select
        AR_CODE,
        sum(case when ACG_CR_DB_F = 'D' and PST_DT = {{ etl_date() }} then abs(nvl(PST_ENTR_AMT_FCY, 0)) else 0 end) as DB_AMT_IN_DAY_FCY,
        count(case when ACG_CR_DB_F = 'D' and PST_DT = {{ etl_date() }} then PST_ENTR_ID end)                     as NBR_OF_DB_IN_DAY,
        sum(case when ACG_CR_DB_F = 'C' and PST_DT = {{ etl_date() }} then abs(nvl(PST_ENTR_AMT_FCY, 0)) else 0 end) as CR_AMT_IN_DAY_FCY,
        count(case when ACG_CR_DB_F = 'C' and PST_DT = {{ etl_date() }} then PST_ENTR_ID end)                     as NBR_OF_CR_IN_DAY,
        sum(case when ACG_CR_DB_F = 'D' and trunc(PST_DT, 'MM') = trunc({{ etl_date() }}, 'MM') then abs(nvl(PST_ENTR_AMT_FCY, 0)) else 0 end) as DB_AMT_IN_MO_FCY,
        count(case when ACG_CR_DB_F = 'D' and trunc(PST_DT, 'MM') = trunc({{ etl_date() }}, 'MM') then PST_ENTR_ID end)                     as NBR_OF_DB_IN_MO,
        sum(case when ACG_CR_DB_F = 'C' and trunc(PST_DT, 'MM') = trunc({{ etl_date() }}, 'MM') then abs(nvl(PST_ENTR_AMT_FCY, 0)) else 0 end) as CR_AMT_IN_MO_FCY,
        count(case when ACG_CR_DB_F = 'C' and trunc(PST_DT, 'MM') = trunc({{ etl_date() }}, 'MM') then PST_ENTR_ID end)                     as NBR_OF_CR_IN_MO,
        sum(case when ACG_CR_DB_F = 'D' then abs(nvl(PST_ENTR_AMT_FCY, 0)) else 0 end) as DB_AMT_IN_YR_FCY,
        count(case when ACG_CR_DB_F = 'D' then PST_ENTR_ID end)                     as NBR_OF_DB_IN_YR,
        sum(case when ACG_CR_DB_F = 'C' then abs(nvl(PST_ENTR_AMT_FCY, 0)) else 0 end) as CR_AMT_IN_YR_FCY,
        count(case when ACG_CR_DB_F = 'C' then PST_ENTR_ID end)                     as NBR_OF_CR_IN_YR
    from {{ ref('pst_entr') }}
    where trunc(PST_DT, 'YYYY') = trunc({{ etl_date() }}, 'YYYY') and PST_DT <= {{ etl_date() }}
    group by AR_CODE
),
dlq as (
    select ORIG_AR_CODE, sum(PNP_ARS) as PNP_ARS
    from {{ ref('ar_dlq_smy') }} where CDR_DT = {{ etl_date() }}
    group by ORIG_AR_CODE
),
la as (select * from {{ ref('loan_ar') }} where SYS_EXP_DT is null)
select
    {{ etl_date() }}                                                     as CDR_DT,
    'SAOKE_CRB'                                                          as SRC_STM_CODE,
    {{ fn_hash('T24_LD_LOANS_AND_DEPOSITS', 'a.APP_ID') }}               as AR_ID,
    cast(a.APP_ID as string)                                             as AR_CODE,
    {{ fn_hash('T24_LD_LOANS_AND_DEPOSITS', "case when a.APP_ID like 'PDLD%' then substr(a.APP_ID, 3, 12) else a.APP_ID end") }} as ORIG_AR_ID,
    case when a.APP_ID like 'PDLD%' then substr(a.APP_ID, 3, 12) else a.APP_ID end as ORIG_AR_CODE,
    cast(sum(case when a.SBV_CODE like '2%' and a.AMOUNT_CUR < 0 then abs(a.AMOUNT_CUR) else 0 end) as decimal(32,5)) as CLS_BAL_FCY,
    cast(sum(case when a.SBV_CODE like '2%' and a.AMOUNT_LCY < 0 then abs(a.AMOUNT_LCY) else 0 end) as decimal(32,5)) as CLS_BAL_LCY,
    cast(greatest(sum(case when a.SBV_CODE like '2%' and a.AMOUNT_CUR < 0 then abs(a.AMOUNT_CUR) else 0 end) - nvl(dlq.PNP_ARS, 0), 0) as decimal(32,5)) as TOT_RECBL_PNP_AMT_FCY,
    cast(sum(case when a.SBV_CODE like '2%' and {{ t24_date('a.MAT_DATE') }} = {{ etl_date() }} and a.AMOUNT_CUR < 0 then abs(a.AMOUNT_CUR) else 0 end) as decimal(32,5)) as TOT_DUE_PNP_AMT_FCY,
    cast(nvl(dlq.PNP_ARS, 0) as decimal(32,5))                           as TOT_ODUE_PNP_AMT_FCY,
    cast(nvl(la.DSBR_AMT_FCY, nvl(la.ORIG_AMT_FCY, 0)) as decimal(32,5)) as DSBR_AMT_LTD_FCY,
    cast(nvl(stmt.DB_AMT_IN_DAY_FCY, 0) as decimal(32,5))                as DB_AMT_IN_DAY_FCY,
    cast(nvl(stmt.NBR_OF_DB_IN_DAY, 0) as decimal(32,5))                 as NBR_OF_DB_IN_DAY,
    cast(nvl(stmt.CR_AMT_IN_DAY_FCY, 0) as decimal(32,5))                as CR_AMT_IN_DAY_FCY,
    cast(nvl(stmt.NBR_OF_CR_IN_DAY, 0) as decimal(32,5))                 as NBR_OF_CR_IN_DAY,
    cast(nvl(stmt.DB_AMT_IN_MO_FCY, 0) as decimal(32,5))                 as DB_AMT_IN_MO_FCY,
    cast(nvl(stmt.NBR_OF_DB_IN_MO, 0) as decimal(32,5))                  as NBR_OF_DB_IN_MO,
    cast(nvl(stmt.CR_AMT_IN_MO_FCY, 0) as decimal(32,5))                 as CR_AMT_IN_MO_FCY,
    cast(nvl(stmt.NBR_OF_CR_IN_MO, 0) as decimal(32,5))                  as NBR_OF_CR_IN_MO,
    cast(nvl(stmt.DB_AMT_IN_YR_FCY, 0) as decimal(32,5))                 as DB_AMT_IN_YR_FCY,
    cast(nvl(stmt.NBR_OF_DB_IN_YR, 0) as decimal(32,5))                  as NBR_OF_DB_IN_YR,
    cast(nvl(stmt.CR_AMT_IN_YR_FCY, 0) as decimal(32,5))                 as CR_AMT_IN_YR_FCY,
    cast(nvl(stmt.NBR_OF_CR_IN_YR, 0) as decimal(32,5))                  as NBR_OF_CR_IN_YR
from a
left join stmt on (case when a.APP_ID like 'PDLD%' then substr(a.APP_ID, 3, 12) else a.APP_ID end) = stmt.AR_CODE
left join dlq  on (case when a.APP_ID like 'PDLD%' then substr(a.APP_ID, 3, 12) else a.APP_ID end) = dlq.ORIG_AR_CODE
left join la   on a.APP_ID = la.AR_CODE
group by a.APP_ID, dlq.PNP_ARS, la.DSBR_AMT_FCY, la.ORIG_AMT_FCY,
         stmt.DB_AMT_IN_DAY_FCY, stmt.NBR_OF_DB_IN_DAY, stmt.CR_AMT_IN_DAY_FCY, stmt.NBR_OF_CR_IN_DAY,
         stmt.DB_AMT_IN_MO_FCY, stmt.NBR_OF_DB_IN_MO, stmt.CR_AMT_IN_MO_FCY, stmt.NBR_OF_CR_IN_MO,
         stmt.DB_AMT_IN_YR_FCY, stmt.NBR_OF_DB_IN_YR, stmt.CR_AMT_IN_YR_FCY, stmt.NBR_OF_CR_IN_YR
