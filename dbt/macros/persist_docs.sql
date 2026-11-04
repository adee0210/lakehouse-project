{# Override dbt-spark: COMMENT cột cho bảng Iceberg (Master: "100% cột có COMMENT").
   - Bản gốc dùng run_query (fetch) -> lỗi 'NoneType' qua Thrift với DDL, và cú pháp CHANGE COLUMN không hợp lệ cho bảng v2.
   - Mỗi ALTER COMMENT tạo 1 schema + metadata.json mới trong Iceberg -> CHỈ alter cột có comment khác hiện tại
     (chạy hằng ngày không sinh thêm metadata, tránh phình metadata làm driver OOM). #}
{% macro spark__alter_column_comment(relation, column_dict) %}
  {%- set current = {} -%}
  {%- set res = run_query('describe table ' ~ relation) -%}
  {%- for row in res.rows -%}
    {%- if row[0] and not row[0].startswith('#') -%}
      {%- do current.update({row[0] | lower: (row[2] or '')}) -%}
    {%- endif -%}
  {%- endfor -%}
  {% for column_name in column_dict %}
    {% set comment = column_dict[column_name]['description'] | replace("'", "") | replace('\\', '') %}
    {% if column_name | lower in current and current[column_name | lower] != comment %}
      {% call statement('comment_' ~ loop.index, fetch_result=False) %}
        alter table {{ relation }} alter column {{ column_name }} comment '{{ comment }}'
      {% endcall %}
    {% endif %}
  {% endfor %}
{% endmacro %}

{% macro spark__alter_relation_comment(relation, relation_comment) %}
  {%- set cmt = relation_comment | replace("'", "") -%}
  {%- set res = run_query("show tblproperties " ~ relation ~ " ('comment')") -%}
  {%- if res.rows | length == 0 or res.rows[0][1] != cmt -%}
    {% call statement('relation_comment', fetch_result=False) %}
      alter table {{ relation }} set tblproperties ('comment' = '{{ cmt }}')
    {% endcall %}
  {%- endif -%}
{% endmacro %}
