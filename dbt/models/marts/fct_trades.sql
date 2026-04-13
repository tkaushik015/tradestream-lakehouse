select
    order_id,
    account_id,
    ticker_symbol as ticker,
    order_type,
    quantity,
    limit_price,
    executed_price,
    coalesce(executed_price, limit_price) as effective_price,
    quantity * coalesce(executed_price, limit_price) as trade_value,
    status,
    placed_at,
    executed_at,
    settled_at,
    updated_at
from {{ source('tradestream', 'trade_orders') }}
