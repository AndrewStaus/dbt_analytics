{{
  config(
    materialized = "table",
    )
}}

with campaigns as (
    select * from {{ ref("int_marketing__campaigns") }}
)

select
    marketing_channel,
    campaign_id,
    campaign_name,
    campaign_start_at,
    loaded_at
from campaigns