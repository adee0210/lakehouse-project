-- [STAGING] Wave 2 | PG_SILVER.AR_X_IP_RLTNP (16 cột) – SCD TYPE 2, 3 nhánh UNION ALL | Sheet 'Arrangement x Involved Party' | Track 2
-- Quan hệ Hợp đồng vay x Bên liên quan: KH vay, Maker, Checker, RM giới thiệu, CB/TP đề xuất & quản lý khoản vay.
-- Bảng trung gian: kết quả biến đổi (ảnh nguồn ${ETL_DATE}) của AR_X_IP_RLTNP được vật lý hoá trước,
-- để bước MERGE SCD2 không phải inline lại toàn bộ kế hoạch nhiều nhánh/JOIN (tránh Spark driver OOM).
{{ config(materialized='table', schema='pg_silver_stg', location_root='s3://silver/_stg', alias='stg_ar_x_ip_rltnp', persist_docs={'relation': false, 'columns': false}) }}


with td as {{ tdate() }},
src as (
    select
        'T24_LD_LOANS_AND_DEPOSITS'                            as SRC_STM_CODE,
        {{ fn_hash('T24_LD_LOANS_AND_DEPOSITS', 'a.RECID') }}  as AR_ID,
        cast(a.RECID as string)                                as AR_CODE,
        {{ fn_hash('T24_CUSTOMER', 'a.CUSTOMER_ID') }}         as CST_ID,
        cast(a.CUSTOMER_ID as string)                          as CST_CODE,
        cast(a.INPUTTER as string)                             as MAKER_OFCR,
        cast(a.AUTHORISER as string)                           as CHKER_OFCR,
        cast(a.MB_RM_BANCHEO as string)                        as REFRRER_OFCR,
        cast(d.CAN_BO_DE_XUAT_GN as string)                    as DSBR_PRPSL_OFCR,
        cast(d.TT_PP_DE_XUAT_GN as string)                     as DSBR_PRPSL_MGR,
        cast(d.CBTD_QL_KHOAN_VAY as string)                    as MGT_OFCR,
        cast(d.TP_PP_QL_KHOAN_VAY as string)                   as MGT_MGR,
        cast(d.CAP_PHE_DUYET_GNTL as string)                   as APRV_AHR
    from {{ bz('t24core_ld_loans_and_deposits') }} a
    left join td d on d.CONTRACT_REF_NO = a.RECID and d.MOV_DT = {{ t24_date('coalesce(a.ORIG_VAL_DATE, a.VALUE_DATE)') }}
    union all
    select
        'T24_LD_LOANS_AND_DEPOSITS_HIS',
        {{ fn_hash('T24_LD_LOANS_AND_DEPOSITS', 'ldh.HIS_AR_CODE') }},
        ldh.HIS_AR_CODE,
        {{ fn_hash('T24_CUSTOMER', 'ldh.CUSTOMER_ID') }},
        cast(ldh.CUSTOMER_ID as string),
        cast(null as string),
        cast(null as string),
        cast(ldh.MB_RM_BANCHEO as string),
        cast(d.CAN_BO_DE_XUAT_GN as string),
        cast(d.TT_PP_DE_XUAT_GN as string),
        cast(d.CBTD_QL_KHOAN_VAY as string),
        cast(d.TP_PP_QL_KHOAN_VAY as string),
        cast(d.CAP_PHE_DUYET_GNTL as string)
    from {{ ldh_latest() }} ldh
    left join td d on d.CONTRACT_REF_NO = ldh.HIS_AR_CODE and d.MOV_DT = {{ t24_date('coalesce(ldh.ORIG_VAL_DATE, ldh.VALUE_DATE)') }}
    union all
    select
        'T24_PD_PAYMENT_DUE',
        {{ fn_hash('T24_LD_LOANS_AND_DEPOSITS', 'substr(d.RECID, 1, 14)') }},
        substr(d.RECID, 1, 14),
        {{ fn_hash('T24_CUSTOMER', 'd.CUSTOMER') }},
        cast(d.CUSTOMER as string),
        cast(d.INPUTTER as string),
        cast(d.AUTHORISER as string),
        cast(null as string), cast(null as string), cast(null as string),
        cast(null as string), cast(null as string), cast(null as string)
    -- spec: LEFT JOIN (SELECT DISTINCT RECID FROM PD_PAYMENT_DUE_MV ...) d1 – không lấy cột nào của d1,
    -- LEFT JOIN với khoá DISTINCT không đổi số dòng => bỏ qua để tối ưu.
    from {{ bz('t24core_pd_payment_due') }} d
    where nvl(d.FLAG_STATUS, '_') <> 'DELETE'
)
select * from src
