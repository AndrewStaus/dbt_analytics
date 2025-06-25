{{
  config(
    materialized = "ephemeral",
    )
}}

with accounts as (
    select * from {{ source("accounts_db", "accounts") }}
)

select
    id         ::STRING    account_id,
    party_key  ::STRING    party_key,
    first_name ::STRING    first_name,
    last_name  ::STRING    last_name,
    loaded_at  ::TIMESTAMP loaded_at
from accounts