{{-
  config(
    materialized = "ephemeral",
    )
-}}

with individual_emails as (
    select * from {{ source("reltio", "individual_emails") }}
)

select
    party_key ::STRING    individual_party_key,
    source    ::STRING    source,
    email     ::STRING    email,
    loaded_at ::TIMESTAMP loaded_at
from individual_emails