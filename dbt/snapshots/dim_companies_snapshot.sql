{% snapshot dim_companies_snapshot %}

{{
    config(
      target_schema='public',
      unique_key='ticker',
      strategy='check',
      check_cols=['cik', 'metric', 'sec_metric', 'unit', 'fiscal_year', 'fiscal_period', 'form_type', 'filed_date']
    )
}}

select distinct
    ticker,
    cik,
    metric,
    sec_metric,
    unit,
    fiscal_year,
    fiscal_period,
    form_type,
    filed_date,
    frame,
    source,
    ingested_at,
    _silver_loaded_at
from {{ source('tradestream', 'company_financials') }}

{% endsnapshot %}
