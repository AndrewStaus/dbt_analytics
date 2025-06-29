{{-
  config(
    materialized = "incremental",
    incremental_strategy="merge",
    unique_key="campaign_sid"
    )
-}}

with
facebook_ads as (
    select * from {{ ref("stg_facebook_ads__campaigns") }}
),
google_ads as (
    select * from {{ ref("stg_google_ads__campaigns") }}
),

united as (
    select
    'facebook_ads' marketing_channel,
    *
    from facebook_ads

    union all

    select
    'google_ads' marketing_channel,
    *
    from google_ads
)

select
{{ dbt_utils.generate_surrogate_key(["marketing_channel", "campaign_id"]) }} campaign_sid,
*
from united

{% if is_incremental() -%}
    where loaded_at >= (select max(loaded_at) from {{ this }})
{%- endif %}