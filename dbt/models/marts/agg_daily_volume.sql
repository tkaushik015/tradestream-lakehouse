select
    ticker,
    date(event_time) as trade_date,
    count(*) as total_trades,
    sum(quantity) as total_quantity,
    sum(quantity * price) as total_trade_value
from {{ source('tradestream', 'trade_orders') }}
group by 1, 2