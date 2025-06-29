{{-
  config(
    materialized = "incremental",
    incremental_strategy="merge",
    unique_key="transaction_id"
    )
-}}

with orders as (
    select * from {{ source("csparc", "transactions") }}
),

conformed as (
select
    channel      ::string        sales_channel,
    date_time    ::timestamp     transacted_at_pst,
    party_key    ::string        individual_party_key,
    order_id     ::int           transaction_id,
    product_id   ::string        product_id,
    revenue/100  ::decimal(12,2) revenue,
    margin/100   ::decimal(12,2) margin,
    loaded_at    ::timestamp     loaded_at
from orders
)

select
    {{ pst_to_utc('transacted_at_pst') }} transacted_at,
    * exclude(transacted_at_pst)
from conformed

{% if is_incremental() -%}
    where loaded_at >= (select max(loaded_at) from {{ this }})
{%- endif %}
qualify row_number() over (partition by transaction_id order by loaded_at desc) = 1

