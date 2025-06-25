{{
  config(
    materialized = "incremental",
    incremental_strategy="merge",
    unique_key="hit_id"
    )
}}

with
web_hits as (
    select 'web' property, * from {{ ref("src_adobe_experience_web_clickstream_hits") }}
),

app_hits as (
    select 'app' property, * from {{ ref("src_adobe_experience_app_clickstream_hits") }}
),

accounts as (
    select * from {{ ref("stg_accounts_db__accounts") }}
),

united_hits as (
select 'web' property, * from web_hits
{% if is_incremental() -%}
    where loaded_at >= (select max(loaded_at) from {{ this }} where property = 'web')
{%- endif %}

union all

select 'app' property, * from app_hits
{% if is_incremental() -%}
    where loaded_at >= (select max(loaded_at) from {{ this }} where property = 'app')
{%- endif %}
),

conformed_hits as (
    select
        {{ pst_to_utc('date_time') }}                   hit_at,
        concat_ws(':', post_visid_high, post_visid_low) visit_id,
        concat_ws(':', visit_id, hitid_high, hitid_low) hit_id,
        mcvisid                                         visitor_id,
        page_url                                        hit_url,
        post_evar133                                    account_id,
        transactionid                                   order_id,
        loaded_at                                       loaded_at
    from united_hits
)

select
    h.* exclude(loaded_at),
    c.individual_party_key,
    h.loaded_at
from conformed_hits h
left join accounts c on h.account_id = c.account_id
qualify row_number() over (partition by hit_id order by h.loaded_at desc) = 1