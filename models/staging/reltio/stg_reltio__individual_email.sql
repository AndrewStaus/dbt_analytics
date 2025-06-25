{{
  config(
    materialized = "incremental",
    incremental_strategy="merge",
    unique_key="individual_party_key"
    )
}}

with individual_party_key as (
    select * from {{ ref("src_reltio_individual_email") }}
)

select
    party_key individual_party_key,
    source,
    {{ norm_hash('email') }} email,
    loaded_at
from individual_party_key
{% if is_incremental() -%}
    where loaded_at >= (select max(loaded_at) from {{ this }})
{%- endif %}
qualify row_number() over (partition by party_key, email order by loaded_at desc) = 1