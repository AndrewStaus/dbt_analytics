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
)

select * from facebook_ads

union all

select * from google_ads