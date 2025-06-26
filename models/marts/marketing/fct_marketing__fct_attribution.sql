{%- set union_table_prefix = "int_marketing__int_attribution_last_click" -%}
{%- set union_table_postfixes = [
        "_30d",
        "_30d_same_brand",
        "_30d_same_sku",
        "_14d",
        "_14d_same_brand",
        "_14d_same_sku"
    ]
-%}
{{-
  config(
    materialized = "view"
    )
-}}

{% for postfix in union_table_postfixes -%}
{% if not loop.first %}
union all
{% endif -%}
select *
from {{ ref(union_table_prefix ~ postfix) }}

{%- endfor %}