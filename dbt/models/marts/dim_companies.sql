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