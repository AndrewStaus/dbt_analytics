{{-
  config(
    materialized = "incremental",
    incremental_strategy="merge",
    unique_key="attribution_sid"
    )
-}}

{{- attribution_last_click_n_days_same_x("14") -}}