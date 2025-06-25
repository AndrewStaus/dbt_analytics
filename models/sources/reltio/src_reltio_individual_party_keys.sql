{{
  config(
    materialized = "ephemeral",
    )
}}

with individual_party_key as (
    select * from {{ source("reltio", "individual_party_keys") }}
)

select
    party_key ::STRING    party_key,
    entity_id ::STRING    entity_id,
    loaded_at ::TIMESTAMP loaded_at
from individual_party_key