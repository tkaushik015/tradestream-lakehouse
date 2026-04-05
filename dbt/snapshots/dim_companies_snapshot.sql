{% snapshot dim_companies_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='company_id',
      strategy='timestamp',
      updated_at='updated_at'
    )
}}

select
    company_id,
    ticker,
    company_name,
    sector,
    industry,
    exchange,
    is_active,
    created_at,
    updated_at
from {{ source('tradestream', 'companies') }}

{% endsnapshot %}