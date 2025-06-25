{% macro latest_record(primary_key, date) -%}

    qualify row_number() over (partition by {{ primary_key }} order by {{ date }} desc) = 1

{%- endmacro %}