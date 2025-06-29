{{-
  config(
    group="marketing",
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='hit_id'
    )
-}}

with
clickstream_hits as (
    select * from {{ ref('stg_adobe_experience__clickstream_hits') }}
),

individual_party_key as (
    select * from {{ ref('stg_reltio__individual_party_keys') }}
),

campaigns as (
    select * from {{ ref('dim_marketing__dim_campaigns') }}
),

split_query_string as (
    select
        h.hit_id,
        h.individual_party_key,
        h.hit_at,
        split(h.hit_url, '?')[1] query_string,
        h.loaded_at
    from clickstream_hits h
),

parsed_hit as (
    select distinct
        hit_id,
        individual_party_key,
        hit_at attribution_start_at,
        regexp_substr(query_string, 'utm_source=([^&]*)', 1, 1, 'e', 1) marketing_channel,
        regexp_substr(query_string, 'utm_campaign=([^&]*)', 1, 1, 'e', 1) campaign_id,
        regexp_substr(query_string, 'product_id=([^&]*)', 1, 1, 'e', 1) advertised_product_id, 
        loaded_at
    from split_query_string
)

select
    c.campaign_sid,
    h.*
from parsed_hit h
inner join campaigns c on true
    and h.marketing_channel = c.marketing_channel
    and h.campaign_id = c.campaign_id

{% if is_incremental() -%}
    where loaded_at >= (select max(loaded_at) from {{ this }})
{%- endif %}