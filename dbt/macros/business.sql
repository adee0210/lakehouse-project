{# Nhánh 2 (LD_HIS) dùng chung cho LOAN_AR, LOAN_AR_PRFL, AR_X_IP_RLTNP, AR_RATE_HIST:
   - lấy version CURR_NO lớn nhất của mỗi hợp đồng (RECID = '<LD_ID>;<CURR_NO>')
   - loại hợp đồng có bất kỳ version REVE (đảo bút toán)
   - loại hợp đồng còn tồn tại trong T24CORE_LD_LOANS_AND_DEPOSITS (đã có ở nhánh 1)
   Trả về các cột LD_HIS + cột HIS_AR_CODE = SUBSTR(RECID,1,INSTR(RECID,';')-1) #}
{% macro ldh_latest() -%}
(
    select h.* from (
        select x.*,
               max(cast(x.CURR_NO as int)) over (partition by x.HIS_AR_CODE) as _max_curr_no,
               max(case when nvl(x.RECORD_STATUS, '_') = 'REVE' then 1 else 0 end) over (partition by x.HIS_AR_CODE) as _has_reve
        from (
            select t.*, substr(t.RECID, 1, instr(t.RECID, ';') - 1) as HIS_AR_CODE
            from {{ bz('t24core_ld_loans_and_deposits_his') }} t
        ) x
    ) h
    left anti join {{ bz('t24core_ld_loans_and_deposits') }} ld on h.HIS_AR_CODE = ld.RECID
    where cast(h.CURR_NO as int) = h._max_curr_no and h._has_reve = 0
)
{%- endmacro %}

{# Bảng FLEXBO_PGB_LOS_CONTRACT_FIELDS_TDATE khử trùng theo (CONTRACT_REF_NO, MOV_DATE) #}
{% macro tdate() -%}
(
    select * from (
        select t.*, {{ t24_date('t.MOV_DATE') }} as MOV_DT,
               row_number() over (partition by t.CONTRACT_REF_NO, {{ t24_date('t.MOV_DATE') }} order by t.LOS_CONTRACT_ID desc) as _rn
        from {{ bz('flexbo_pgb_los_contract_fields_tdate') }} t
    ) where _rn = 1
)
{%- endmacro %}

{# Lấy 1 dòng mới nhất / khoá từ một relation (chống fan-out khi LEFT JOIN) #}
{% macro latest_by(relation_sql, partition_cols, order_by) -%}
(
    select * from (
        select z.*, row_number() over (partition by {{ partition_cols }} order by {{ order_by }}) as _rn
        from {{ relation_sql }} z
    ) where _rn = 1
)
{%- endmacro %}

{# Ngày làm thay đổi bản ghi Silver dùng cho Surrogate Key Gold: COALESCE(SYS_UDT_DT, SYS_EFF_DT, ${ETL_DATE}) #}
{% macro etl_change_date(alias) -%}
    coalesce({{ alias }}.SYS_UDT_DT, {{ alias }}.SYS_EFF_DT, {{ etl_date() }})
{%- endmacro %}

{# DIM_KEY = CONCAT(TO_CHAR(ETL_CHANGE_DATE,'YYYYMMDD'), <ID>) #}
{% macro dim_key(change_date_expr, id_expr) -%}
    concat(date_format({{ change_date_expr }}, 'yyyyMMdd'), {{ id_expr }})
{%- endmacro %}
