select
    ticker,
    trade_date,
    count(*) as total_price_events,
    sum(volume) as total_volume,
    avg(price) as avg_price,
    min(low) as day_low,
    max(high) as day_high
from {{ source('tradestream', 'price_snapshots') }}
group by 1, 2
