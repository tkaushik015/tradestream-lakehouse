select
    trade_id,
    order_id,
    company_id,
    ticker,
    order_type,
    side,
    quantity,
    price,
    quantity * price as trade_value,
    order_status,
    event_time,
    created_at
from {{ source('tradestream', 'trade_orders') }}