{{
  config(
    materialized = "ephemeral",
    )
}}

with
facebook_ads as (
    select * from {{ ref("stg_facebook_ads__campaigns") }}
),
google_ads as (
    select * from {{ ref("stg_google_ads__campaigns") }}
),

united as (
    select * from facebook_ads
    union all
    select * from google_ads
)

select
{{ generate_sid(["marketing_channel", "campaign_id"]) }} campaign_sid,
*
from united