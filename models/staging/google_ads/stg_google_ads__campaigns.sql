{{
  config(
    materialized = "incremental",
    incremental_strategy="merge",
    unique_key="campaign_id"
    )
-}}

with
campaigns as (
    select * from {{ ref("src_google_ads_campaigns") }}
),
campaigns_conformed as (
    select
        marketing_channel marketing_channel,
        id                campaign_id,
        name              campaign_name,
        campaign_start_at campaign_start_at,
        loaded_at         loaded_at
    from campaigns
)

select
    {{ generate_sid(["marketing_channel", "campaign_id"]) }} campaign_sid,
    *
from campaigns_conformed
{% if is_incremental() -%}
    where loaded_at >= (select max(loaded_at) from {{ this }})
{%- endif %}
qualify row_number() over (partition by campaign_id order by loaded_at desc) = 1