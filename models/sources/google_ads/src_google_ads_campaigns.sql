{{
  config(
    materialized = "ephemeral",
    )
}}

with campaigns as (
    select * from {{ source("google_ads", "campaigns") }}
)

select
    'google_ads'                          ::STRING    marketing_channel,
    JSON_EXTRACT_PATH_TEXT(DATA, 'id')    ::INT       id,
    JSON_EXTRACT_PATH_TEXT(DATA, 'name')  ::STRING    name,
    JSON_EXTRACT_PATH_TEXT(DATA, 'start') ::TIMESTAMP campaign_start_at,
    loaded_at                             ::TIMESTAMP loaded_at
from campaigns