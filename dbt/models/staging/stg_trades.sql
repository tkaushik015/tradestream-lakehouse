select
    order_id,
    account_id,
    ticker_symbol as ticker,
    order_type,
    quantity,
    limit_price,
    executed_price,
    status,
    placed_at,
    executed_at,
    settled_at,
    updated_at
from {{ source('tradestream', 'trade_orders') }}
