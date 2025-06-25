{{
  config(
    materialized = "ephemeral",
    )
}}

with orders as (
    select * from {{ source("csparc", "transactions") }}
)

select
    channel      ::STRING        channel,
    date_time    ::TIMESTAMP     date_time,
    party_key    ::STRING        party_key,
    order_id     ::INT           order_id,
    product_id   ::STRING        product_id,
    revenue/100  ::DECIMAL(12,2) revenue,
    margin/100   ::DECIMAL(12,2) margin,
    loaded_at    ::TIMESTAMP     loaded_at
from orders