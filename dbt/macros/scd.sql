{# =========================================================================
   Chiến lược load (Master guide – Phần II, dòng TYPE)
   Model khai báo CTE `src` chứa ảnh dữ liệu nguồn tại ${ETL_DATE}
   (chỉ cột nghiệp vụ, KHÔNG có SYS_EFF_DT/SYS_EXP_DT/SYS_UDT_DT), rồi gọi:
     {{ scd1(['CST_ID']) }}   -> materialized=incremental, strategy=merge, unique_key=keys
     {{ scd2(['AR_ID']) }}    -> materialized=incremental, strategy=merge, unique_key=keys+SYS_EFF_DT
     {{ dim_scd2() }}         -> Gold DIM_*: merge theo DIM_KEY
   ========================================================================= #}


{% macro _on(keys, l, r) -%}
    {%- for k in keys %}{{ l }}.{{ k }} = {{ r }}.{{ k }}{% if not loop.last %} and {% endif %}{% endfor -%}
{%- endmacro %}

{% macro _cols(alias, cols) -%}
    {%- for c in cols %}{{ alias }}.{{ c }}{% if not loop.last %}, {% endif %}{% endfor -%}
{%- endmacro %}

{# Khử trùng khoá nghiệp vụ (an toàn cho MERGE) + bỏ dòng khoá NULL #}
{% macro _dedup_src(keys) -%}
src_d as (
    select d.r.* from (
        select max(struct(s.*)) as r
        from src s
        where {% for k in keys %}s.{{ k }} is not null{% if not loop.last %} and {% endif %}{% endfor %}
        group by {{ _cols('s', keys) }}
    ) d
)
{%- endmacro %}

{# ----------------------------------------------------------------- SCD TYPE 1
   Merge theo khoá chính, ghi đè giá trị mới nhất; giữ SYS_EFF_DT lần đầu;
   SYS_UDT_DT = ${ETL_DATE} khi bản ghi thay đổi; bản ghi biến mất khỏi nguồn
   -> SYS_EXP_DT = ${ETL_DATE} (soft delete). #}
{% macro scd1(keys) -%}
{%- set biz = existing_business_columns(['SYS_EFF_DT', 'SYS_EXP_DT', 'SYS_UDT_DT']) -%}
, {{ _dedup_src(keys) }}
{% if is_incremental() and biz | length > 0 %}
, s_h as (select s.*, md5(to_json(struct({{ _cols('s', biz) }}))) as _hd from src_d s)
, t_h as (select t.*, md5(to_json(struct({{ _cols('t', biz) }}))) as _hd from {{ this }} t)
select {{ _cols('s', biz) }},
       coalesce(t.SYS_EFF_DT, {{ etl_date() }}) as SYS_EFF_DT,
       cast(null as date) as SYS_EXP_DT,
       {{ etl_date() }} as SYS_UDT_DT
from s_h s
left join t_h t on {{ _on(keys, 's', 't') }}
where t.{{ keys[0] }} is null or s._hd <> t._hd or t.SYS_EXP_DT is not null
union all
select {{ _cols('t', biz) }}, t.SYS_EFF_DT, {{ etl_date() }} as SYS_EXP_DT, {{ etl_date() }} as SYS_UDT_DT
from t_h t
left anti join s_h s on {{ _on(keys, 't', 's') }}
where t.SYS_EXP_DT is null
{% else %}
select s.*, {{ etl_date() }} as SYS_EFF_DT, cast(null as date) as SYS_EXP_DT, {{ etl_date() }} as SYS_UDT_DT
from src_d s
{% endif %}
{%- endmacro %}

{# ----------------------------------------------------------------- SCD TYPE 2
   Có thay đổi thuộc tính -> đóng dòng cũ (SYS_EXP_DT = ${ETL_DATE}) và chèn dòng
   mới (SYS_EFF_DT = ${ETL_DATE}, SYS_EXP_DT = NULL). Mỗi business key chỉ có
   đúng 1 dòng hiệu lực (SYS_EXP_DT IS NULL). Chạy lại cùng ngày: idempotent. #}
{% macro scd2(keys) -%}
{%- set biz = existing_business_columns(['SYS_EFF_DT', 'SYS_EXP_DT', 'SYS_UDT_DT']) -%}
, {{ _dedup_src(keys) }}
{% if is_incremental() and biz | length > 0 %}
, s_h as (select s.*, md5(to_json(struct({{ _cols('s', biz) }}))) as _hd from src_d s)
, t_h as (select t.*, md5(to_json(struct({{ _cols('t', biz) }}))) as _hd from {{ this }} t where t.SYS_EXP_DT is null)
-- (1) đóng phiên bản hiệu lực cũ: bị xoá khỏi nguồn hoặc có thay đổi
select {{ _cols('t', biz) }}, t.SYS_EFF_DT, {{ etl_date() }} as SYS_EXP_DT, {{ etl_date() }} as SYS_UDT_DT
from t_h t
left join s_h s on {{ _on(keys, 't', 's') }}
where s.{{ keys[0] }} is null
   or (s._hd <> t._hd and t.SYS_EFF_DT <> {{ etl_date() }})
union all
-- (2) mở phiên bản mới: bản ghi mới hoặc có thay đổi
select {{ _cols('s', biz) }}, {{ etl_date() }} as SYS_EFF_DT, cast(null as date) as SYS_EXP_DT, {{ etl_date() }} as SYS_UDT_DT
from s_h s
left join t_h t on {{ _on(keys, 's', 't') }}
where t.{{ keys[0] }} is null or s._hd <> t._hd
{% else %}
select s.*, {{ etl_date() }} as SYS_EFF_DT, cast(null as date) as SYS_EXP_DT, {{ etl_date() }} as SYS_UDT_DT
from src_d s
{% endif %}
{%- endmacro %}

{# ----------------------------------------------------------------- Gold DIM (SCD2)
   CTE `src` phải có: DIM_KEY, <ID>, thuộc tính..., SYS_EFF_DT, SYS_EXP_DT
   DIM_KEY = CONCAT(TO_CHAR(ETL_CHANGE_DATE,'YYYYMMDD'), ID),
   ETL_CHANGE_DATE = COALESCE(SYS_UDT_DT, SYS_EFF_DT, ${ETL_DATE}).
   Merge theo DIM_KEY; phiên bản cũ (ID đổi DIM_KEY hoặc biến mất) bị đóng. #}
{% macro dim_scd2(id_col) -%}
{%- set allc = existing_business_columns([]) -%}
, src_d as (
    select d.r.* from (select max(struct(s.*)) as r from src s where s.DIM_KEY is not null group by s.DIM_KEY) d
)
{% if is_incremental() and allc | length > 0 %}
select {% for c in allc %}{% if c | upper == 'SYS_EXP_DT' %}{{ etl_date() }} as SYS_EXP_DT{% else %}t.{{ c }}{% endif %}{% if not loop.last %}, {% endif %}{% endfor %}
from {{ this }} t
left join src_d s on t.{{ id_col }} = s.{{ id_col }}
where t.SYS_EXP_DT is null and (s.{{ id_col }} is null or s.DIM_KEY <> t.DIM_KEY)
union all
select {{ _cols('s', allc) }} from src_d s
{% else %}
select * from src_d
{% endif %}
{%- endmacro %}
