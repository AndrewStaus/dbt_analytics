{{-
  config(
    tags=["first_name", "last_name", "account_id"],
    materialized="ephemeral",
    )
-}}

with accounts as (
    select * from {{ source("accounts_db", "accounts") }}
)

select
    id         ::string    account_id,
    party_key  ::string    individual_party_key,
    first_name ::string    first_name,
    last_name  ::string    last_name,
    loaded_at  ::timestamp loaded_at
from accounts