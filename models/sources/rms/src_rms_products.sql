{{
  config(
    materialized = "ephemeral",
    )
}}

with individual_party_key as (
    select * from {{ source("rms", "products") }}
)

select
    id        ::string    id,
    name      ::string    name,
    brand     ::string    brand,
    loaded_at ::timestamp loaded_at
from individual_party_key