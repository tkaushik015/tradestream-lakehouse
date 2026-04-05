select
    price_id,
    ticker,
    market_price,
    event_time,
    source,
    created_at
from {{ source('tradestream', 'market_prices') }}