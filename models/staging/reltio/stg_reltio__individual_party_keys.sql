{{-
  config(
    materialized = "incremental",
    incremental_strategy="merge",
    unique_key="individual_party_key"
    )
-}}

with individual_party_keys as (
    select * from {{ source("reltio", "individual_party_keys") }}
)

select
    party_key ::STRING    individual_party_key,
    entity_id ::STRING    individual_entity_id,
    loaded_at ::TIMESTAMP loaded_at
from individual_party_keys

{% if is_incremental() -%}
    where loaded_at >= (select max(loaded_at) from {{ this }})
{%- endif %}
qualify row_number() over (partition by individual_party_key order by loaded_at desc) = 1