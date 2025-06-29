{{-
  config(
    materialized="incremental",
    incremental_strategy="merge",
    unique_key="campaign_id"
    )
-}}

with campaigns as (
    select * from {{ source("facebook_ads", "campaigns") }}
)

select
    data:id         ::int       campaign_id,
    data:name       ::string    campaign_name,
    data:start_date ::timestamp campaign_start_at,
    loaded_at       ::timestamp loaded_at
from campaigns

{% if is_incremental() -%}
    where loaded_at >= (select max(loaded_at) from {{ this }})
{%- endif %}
qualify row_number() over (partition by campaign_id order by loaded_at desc) = 1
