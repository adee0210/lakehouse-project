{# =========================================================================
   Quy ước chung của dự án (Master guide – Phần II)
   ========================================================================= #}

{# ${ETL_DATE}: biến ngày chạy, truyền bằng --vars '{etl_date: 2025-11-26}' #}
{% macro etl_date_str() -%}
    {{ return(var('etl_date')) }}
{%- endmacro %}

{% macro etl_date() -%}
    DATE'{{ var("etl_date") }}'
{%- endmacro %}

{# fn_hash("PREFIX", COL): khoá CHAR(32) = MD5(PREFIX || COL), KHÔNG có dấu '|'.
   COL NULL -> NULL (Master: kiểm tra IS NOT NULL trước khi băm). #}
{% macro fn_hash(prefix, col) -%}
    md5(concat('{{ prefix }}', cast({{ col }} as string)))
{%- endmacro %}

{# Chuyển ngày T24 về DATE. Nguồn T24 có thể lưu DATE thật, chuỗi 'YYYYMMDD'
   hoặc DATE_TIME dạng 'YYMMDDHH24MI' -> trả về DATE, sai định dạng -> NULL. #}
{#  Các dạng gặp trên Oracle thật: '20250901' | 20320105 (NUMBER) | '2508201251' (DATE_TIME YYMMDDHH24MI)
    | '2025-10-07 00:00:00.000' | DATE | đa trị T24 '20260215#20260115#...' (lấy giá trị đầu = SUBSTR(...,1,8)) #}
{% macro t24_date(col) -%}
    (case
        when {{ col }} is null then null
        when regexp_replace(cast({{ col }} as string), '\\.0*$', '') rlike '^[0-9]{10}$'
            then to_date(substr(cast({{ col }} as string), 1, 6), 'yyMMdd')
        when cast({{ col }} as string) rlike '^[0-9]{8}([^0-9]|$)'
            then to_date(substr(cast({{ col }} as string), 1, 8), 'yyyyMMdd')
        else to_date(substr(cast({{ col }} as string), 1, 10))
     end)
{%- endmacro %}

{# Đọc 1 bảng Bronze tại đúng phân vùng ngày chạy (ETL_DATE = ${ETL_DATE}) #}
{% macro bz(model_name) -%}
    (select * from {{ ref(model_name) }} where ETL_DATE = {{ etl_date() }})
{%- endmacro %}

{# Schema nguồn cho dbt source: mock -> namespace Iceberg src_<schema>;
   oracle -> bảng JDBC đăng ký trong spark_catalog.ora_<schema> #}
{% macro source_schema(name) -%}
    {%- if var('source_mode', 'mock') == 'oracle' -%}
        spark_catalog.ora_{{ name }}
    {%- else -%}
        src_{{ name }}
    {%- endif -%}
{%- endmacro %}

{# Tên schema = đúng custom schema (pg_bronze / pg_silver / pg_gold) #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ (custom_schema_name or target.schema) | trim }}
{%- endmacro %}

{# Xoá phân vùng ngày chạy trước khi append (idempotent khi chạy lại cùng ngày).
   Chỉ chạy khi bảng đã tồn tại. #}
{% macro delete_partition(col) -%}
    {%- set rel = adapter.get_relation(this.database, this.schema, this.identifier) -%}
    {%- if rel is not none and not flags.FULL_REFRESH -%}
        delete from {{ this }} where {{ col }} = {{ etl_date() }}
    {%- else -%}
        select 1
    {%- endif -%}
{%- endmacro %}

{# Danh sách cột của chính model (khi đã tồn tại) trừ các cột hệ thống #}
{% macro existing_business_columns(exclude=[]) -%}
    {%- set rel = adapter.get_relation(this.database, this.schema, this.identifier) -%}
    {%- if rel is none -%}{{ return([]) }}{%- endif -%}
    {%- set cols = [] -%}
    {%- for c in adapter.get_columns_in_relation(rel) -%}
        {%- if c.name | upper not in (exclude | map('upper') | list) -%}{%- do cols.append(c.name) -%}{%- endif -%}
    {%- endfor -%}
    {{ return(cols) }}
{%- endmacro %}
