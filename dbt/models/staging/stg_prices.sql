select
    ticker,
    price,
    volume,
    high,
    low,
    open,
    source,
    event_timestamp,
    trade_date,
    _silver_loaded_at
from {{ source('tradestream', 'price_snapshots') }}
