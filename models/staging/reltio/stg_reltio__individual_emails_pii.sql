{{-
  config(
    tags=["email"],
    materialized = "incremental",
    incremental_strategy="merge",
    unique_key="individual_party_key"
    )
-}}

with individual_emails as (
    select * from {{ ref("src_reltio_individual_emails") }}
)

select
    individual_party_key,
    {{ normalize('email') }} email,
    loaded_at
from individual_emails
{% if is_incremental() -%}
    where loaded_at >= (select max(loaded_at) from {{ this }})
{%- endif %}
qualify row_number() over (partition by individual_party_key, email order by loaded_at desc) = 1