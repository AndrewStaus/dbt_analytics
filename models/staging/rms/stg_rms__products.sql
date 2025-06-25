{{
  config(
    materialized = "incremental",
    incremental_strategy="merge",
    unique_key="product_id"
    )
}}

with products as (
    select * from {{ ref("src_rms_products") }}
)

select
    id        product_id,
    name      product_name,
    brand     brand_name,
    loaded_at loaded_at
from products

{% if is_incremental() -%}
    where loaded_at >= (select max(loaded_at) from {{ this }})
{%- endif %}
qualify row_number() over (partition by product_id order by loaded_at desc) = 1