{{-
  config(
    materialized = "incremental",
    incremental_strategy="merge",
    unique_key="account_id"
    )
-}}

with accounts as (
    select * from {{ ref("src_accounts_db_accounts") }}
)

select
    account_id,
    individual_party_key,
    {{ norm_hash('first_name') }} first_name,
    {{ norm_hash('last_name') }} last_name,
    loaded_at
from accounts

{% if is_incremental() -%}
    where loaded_at >= (select max(loaded_at) from {{ this }} )
{%- endif %}
{{ latest_record('account_id', 'loaded_at') }}