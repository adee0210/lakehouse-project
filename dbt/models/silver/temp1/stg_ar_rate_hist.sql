-- [STAGING] Wave 1 | PG_SILVER.AR_RATE_HIST (12 cột) – SCD TYPE 2, 3 nhánh UNION ALL | Sheet 'Arrangement Rate History' | Track 6
-- Nhánh 1: LD hiện hành | Nhánh 2: LD_HIS (version cuối, không REVE, không còn trong LD) | Nhánh 3: PD quá hạn
-- AR_CODE chuẩn hoá: nhánh 2 SUBSTR(RECID,1,INSTR(RECID,';')-1), nhánh 3 SUBSTR(d.RECID,1,14)
-- Bảng trung gian: kết quả biến đổi (ảnh nguồn ${ETL_DATE}) của AR_RATE_HIST được vật lý hoá trước,
-- để bước MERGE SCD2 không phải inline lại toàn bộ kế hoạch nhiều nhánh/JOIN (tránh Spark driver OOM).
{{ config(materialized='table', schema='pg_silver_stg', location_root='s3://silver/_stg', alias='stg_ar_rate_hist', persist_docs={'relation': false, 'columns': false}) }}


with td as {{ tdate() }},
src as (
    select
        {{ fn_hash('T24_LD_LOANS_AND_DEPOSITS', 'a.RECID') }}  as AR_ID,
        cast(a.RECID as string)                                as AR_CODE,
        cast(a.INT_RATE_TYPE as string)                        as INT_RATE_VARIABLITY_TP_CODE,
        {{ t24_date('a.INT_VALUE_DATE') }}                     as INT_RATE_EFF_DT,
        cast(a.INTEREST_RATE as decimal(25,7))                 as EFF_RATE_PCT,
        cast(a.INTEREST_SPREAD as decimal(25,7))               as SPRD_RATE_PCT,
        cast(a.PE_RATE as decimal(25,7))                       as PNY_RATE,
        cast(a.PS_RATE as decimal(25,7))                       as ODUE_INT_RATE,
        cast(d.BIEN_DO_THAY_DOI_LS_THEO_HDTD as decimal(25,7)) as MRGN_RATE
    from {{ bz('t24core_ld_loans_and_deposits') }} a
    left join td d on d.CONTRACT_REF_NO = a.RECID
                  and d.MOV_DT = {{ t24_date('coalesce(a.ORIG_VAL_DATE, a.VALUE_DATE)') }}
    union all
    select
        {{ fn_hash('T24_LD_LOANS_AND_DEPOSITS', 'ldh.HIS_AR_CODE') }},
        ldh.HIS_AR_CODE,
        cast(ldh.INT_RATE_TYPE as string),
        {{ t24_date('ldh.INT_VALUE_DATE') }},
        cast(ldh.INTEREST_RATE as decimal(25,7)),
        cast(ldh.INTEREST_SPREAD as decimal(25,7)),
        cast(null as decimal(25,7)),
        cast(null as decimal(25,7)),
        cast(d.BIEN_DO_THAY_DOI_LS_THEO_HDTD as decimal(25,7))
    from {{ ldh_latest() }} ldh
    left join td d on d.CONTRACT_REF_NO = ldh.HIS_AR_CODE
                  and d.MOV_DT = {{ t24_date('coalesce(ldh.ORIG_VAL_DATE, ldh.VALUE_DATE)') }}
    union all
    select
        {{ fn_hash('T24_LD_LOANS_AND_DEPOSITS', 'substr(d.RECID, 1, 14)') }},
        substr(d.RECID, 1, 14),
        cast(null as string),
        cast(null as date),
        cast(null as decimal(25,7)),
        cast(null as decimal(25,7)),
        cast(d.PE_RATE as decimal(25,7)),
        cast(d.PS_RATE as decimal(25,7)),
        cast(null as decimal(25,7))
    from {{ bz('t24core_pd_payment_due') }} d
)
select * from src
