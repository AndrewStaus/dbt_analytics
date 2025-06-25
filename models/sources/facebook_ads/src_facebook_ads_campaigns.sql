{{
  config(
    materialized = "ephemeral",
    )
}}

with campaigns as (
    select * from {{ source("facebook_ads", "campaigns") }}
)

select
    'facebook_ads'                             ::STRING    marketing_channel,
    JSON_EXTRACT_PATH_TEXT(DATA, 'id')         ::INT       id,
    JSON_EXTRACT_PATH_TEXT(DATA, 'name')       ::STRING    name,
    JSON_EXTRACT_PATH_TEXT(DATA, 'start_date') ::TIMESTAMP start_date,
    loaded_at                                  ::TIMESTAMP loaded_at
from campaigns