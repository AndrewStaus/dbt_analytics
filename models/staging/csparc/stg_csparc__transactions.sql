{{
  config(
    materialized = "incremental",
    incremental_strategy="merge",
    unique_key="transaction_id"
    )
}}

with orders as (
    select * from {{ ref("src_csparc_transactions") }}
)

select
    {{ pst_to_utc('date_time') }} transacted_at,
    channel                       sales_channel,
    party_key                     individual_party_key,
    order_id                      transaction_id,
    product_id                    product_id,
    revenue                       revenue,
    margin                        margin,
    loaded_at                     loaded_at
from orders

{% if is_incremental() -%}
    where loaded_at >= (select max(loaded_at) from {{ this }})
{%- endif %}
qualify row_number() over (partition by transaction_id order by loaded_at desc) = 1

