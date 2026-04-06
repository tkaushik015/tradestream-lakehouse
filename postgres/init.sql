CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS trading_accounts (
    account_id      VARCHAR(20) PRIMARY KEY,
    account_name    VARCHAR(100) NOT NULL,
    account_type    VARCHAR(10) NOT NULL CHECK (account_type IN ('RETAIL', 'MARGIN', 'IRA')),
    cash_balance    DECIMAL(15,2) NOT NULL DEFAULT 0.00,
    buying_power    DECIMAL(15,2) NOT NULL DEFAULT 0.00,
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS trade_orders (
    order_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id      VARCHAR(20) NOT NULL REFERENCES trading_accounts(account_id),
    ticker_symbol   VARCHAR(10) NOT NULL,
    order_type      VARCHAR(4) NOT NULL CHECK (order_type IN ('BUY', 'SELL')),
    quantity        INTEGER NOT NULL CHECK (quantity > 0),
    limit_price     DECIMAL(10,2),
    executed_price  DECIMAL(10,2),
    status          VARCHAR(12) NOT NULL DEFAULT 'PENDING'
                    CHECK (status IN ('PENDING', 'EXECUTED', 'SETTLED', 'CANCELLED')),
    placed_at       TIMESTAMP NOT NULL DEFAULT NOW(),
    executed_at     TIMESTAMP,
    settled_at      TIMESTAMP,
    updated_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS portfolio_positions (
    position_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id      VARCHAR(20) NOT NULL REFERENCES trading_accounts(account_id),
    ticker_symbol   VARCHAR(10) NOT NULL,
    quantity        INTEGER NOT NULL DEFAULT 0,
    avg_cost_basis  DECIMAL(10,2) NOT NULL DEFAULT 0.00,
    unrealized_pnl  DECIMAL(12,2) NOT NULL DEFAULT 0.00,
    updated_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE(account_id, ticker_symbol)
);

CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_orders_updated_at ON trade_orders;
CREATE TRIGGER trg_orders_updated_at
BEFORE UPDATE ON trade_orders
FOR EACH ROW EXECUTE FUNCTION update_updated_at();

DROP TRIGGER IF EXISTS trg_positions_updated_at ON portfolio_positions;
CREATE TRIGGER trg_positions_updated_at
BEFORE UPDATE ON portfolio_positions
FOR EACH ROW EXECUTE FUNCTION update_updated_at();

DROP TRIGGER IF EXISTS trg_accounts_updated_at ON trading_accounts;
CREATE TRIGGER trg_accounts_updated_at
BEFORE UPDATE ON trading_accounts
FOR EACH ROW EXECUTE FUNCTION update_updated_at();

CREATE INDEX IF NOT EXISTS idx_orders_account_id ON trade_orders(account_id);
CREATE INDEX IF NOT EXISTS idx_orders_ticker ON trade_orders(ticker_symbol);
CREATE INDEX IF NOT EXISTS idx_orders_status ON trade_orders(status);
CREATE INDEX IF NOT EXISTS idx_orders_placed_at ON trade_orders(placed_at);
CREATE INDEX IF NOT EXISTS idx_positions_account ON portfolio_positions(account_id);

INSERT INTO trading_accounts (account_id, account_name, account_type, cash_balance, buying_power)
VALUES
    ('ACC000001', 'Alice Johnson', 'RETAIL', 25000.00, 50000.00),
    ('ACC000002', 'Bob Smith', 'MARGIN', 40000.00, 80000.00),
    ('ACC000003', 'Carol Davis', 'IRA', 15000.00, 15000.00),
    ('ACC000004', 'David Wilson', 'RETAIL', 32000.00, 32000.00),
    ('ACC000005', 'Emma Brown', 'MARGIN', 60000.00, 120000.00)
ON CONFLICT (account_id) DO NOTHING;
