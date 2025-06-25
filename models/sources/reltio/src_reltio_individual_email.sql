{{
  config(
    materialized = "ephemeral",
    )
}}

with individual_party_key as (
    select * from {{ source("reltio", "individual_email") }}
)

select
    party_key ::STRING    party_key,
    source    ::STRING    source,
    email     ::STRING    email,
    loaded_at ::TIMESTAMP loaded_at
from individual_party_key