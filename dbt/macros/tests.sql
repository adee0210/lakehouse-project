{# =========================================================================
   Generic data tests – Master guide Bước 6 (Reconciliation & nghiệm thu)
   ========================================================================= #}

{# (a) Không trùng khoá chính. where: lọc phân vùng ngày chạy cho bảng snapshot. #}
{% test unique_key(model, columns, where=none) %}
select {{ columns | join(', ') }}, count(*) as n
from {{ model }}
{% if where %}where {{ where }}{% endif %}
group by {{ columns | join(', ') }}
having count(*) > 1
{% endtest %}

{# SCD2: mỗi business key chỉ có đúng 1 dòng hiệu lực (SYS_EXP_DT IS NULL) #}
{% test scd2_single_current(model, columns) %}
select {{ columns | join(', ') }}, count(*) as n
from {{ model }}
where SYS_EXP_DT is null
group by {{ columns | join(', ') }}
having count(*) > 1
{% endtest %}

{# SCD2: SYS_EFF_DT <= SYS_EXP_DT #}
{% test scd2_valid_period(model) %}
select * from {{ model }} where SYS_EXP_DT is not null and SYS_EXP_DT < SYS_EFF_DT
{% endtest %}

{# (b) Tỷ lệ khớp khoá fn_hash khi join sang bảng khác >= min_rate (mặc định 99.9%) #}
{% test hash_match_rate(model, column_name, to, field, min_rate=0.999, where=none, to_where=none) %}
with s as (
    select {{ column_name }} as k from {{ model }}
    where {{ column_name }} is not null {% if where %}and {{ where }}{% endif %}
),
t as (select distinct {{ field }} as k from {{ to }} {% if to_where %}where {{ to_where }}{% endif %}),
r as (
    select count(*) as total, count(t.k) as matched
    from s left join t on s.k = t.k
)
select * from r where total > 0 and matched / total < {{ min_rate }}
{% endtest %}

{# Khoá CHAR(32) = md5 hex #}
{% test is_md5(model, column_name) %}
select {{ column_name }} from {{ model }}
where {{ column_name }} is not null and not ({{ column_name }} rlike '^[0-9a-f]{32}$')
{% endtest %}

{# Bảng có dữ liệu cho ngày chạy #}
{% test has_rows(model, where=none) %}
select 1 as no_rows from (select count(*) as n from {{ model }} {% if where %}where {{ where }}{% endif %}) x where n = 0
{% endtest %}

{# Cấu trúc bảng khớp đúng danh sách cột thiết kế trong BA (thứ tự + số lượng) #}
{% test column_contract(model, expected) %}
{%- set actual = adapter.get_columns_in_relation(model) | map(attribute='name') | map('upper') | list -%}
{%- set exp = expected | map('upper') | list -%}
{%- if actual != exp %}
select '{{ actual | join(",") }}' as actual_columns, '{{ exp | join(",") }}' as expected_columns
{%- else %}
select 1 as ok where 1 = 0
{%- endif %}
{% endtest %}
