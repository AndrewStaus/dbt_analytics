{{-
  config(
    materialized="incremental",
    incremental_strategy="delete+insert",
    unique_key='hit_id'
    )
-}}

with app_hits as (
    select * from {{ source("adobe_experience_app", "clickstream_hits") }}
),

web_hits as (
    select * from {{ source("adobe_experience_web", "clickstream_hits") }}
),

accounts as (
    select * from {{ ref("stg_accounts_db__accounts") }}
),

united_hits as (
    select 'app' property, * from app_hits
    {% if is_incremental() -%}
        where loaded_at >= (select max(loaded_at) from {{ this }} where property = 'web')
    {%- endif %}

    union all

    select 'web' property, * from web_hits
    {% if is_incremental() -%}
        where loaded_at >= (select max(loaded_at) from {{ this }} where property = 'web')
    {%- endif %}
),

conformed_hits as (
    select
        property                                        ::string    property,
        concat_ws(':', post_visid_high, post_visid_low) ::string    visit_id,
        concat_ws(':', visit_id, hitid_high, hitid_low) ::string    hit_id,
        mcvisid                                         ::string    visitor_id,
        page_url                                        ::string    hit_url,
        post_evar133                                    ::string    account_id,
        transactionid                                   ::int       order_id,
        {{ pst_to_utc('date_time') }}                   ::timestamp hit_at,
        loaded_at                                       ::timestamp loaded_at
    from united_hits
)

select
    h.*,
    a.individual_party_key
from conformed_hits h
left join accounts a on h.account_id = a.account_id