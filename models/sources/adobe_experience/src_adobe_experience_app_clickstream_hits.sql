{{
  config(
    materialized = "ephemeral",
    )
}}

with app_hits as (
    select * from {{ source("adobe_experience", "app_clickstream_hits") }}
)

select
    post_visid_high ::string    post_visid_high,
    post_visid_low  ::string    post_visid_low,
    hitid_high      ::string    hitid_high,
    hitid_low       ::string    hitid_low,
    date_time       ::timestamp date_time,
    mcvisid         ::string    mcvisid,
    page_url        ::string    page_url,
    post_evar133    ::string    post_evar133,
    transactionid   ::int       transactionid,
    loaded_at       ::timestamp loaded_at
from app_hits