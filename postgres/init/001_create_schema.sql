CREATE TABLE IF NOT EXISTS companies (
    company_id SERIAL PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL UNIQUE,
    company_name VARCHAR(255) NOT NULL,
    sector VARCHAR(100),
    industry VARCHAR(100),
    exchange VARCHAR(50),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS trade_orders (
    trade_id BIGSERIAL PRIMARY KEY,
    order_id VARCHAR(50) NOT NULL UNIQUE,
    company_id INT NOT NULL REFERENCES companies(company_id),
    ticker VARCHAR(10) NOT NULL,
    order_type VARCHAR(10) NOT NULL,
    side VARCHAR(10) NOT NULL,
    quantity INT NOT NULL,
    price NUMERIC(18,4) NOT NULL,
    order_status VARCHAR(20) NOT NULL,
    event_time TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS market_prices (
    price_id BIGSERIAL PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL,
    market_price NUMERIC(18,4) NOT NULL,
    event_time TIMESTAMP NOT NULL,
    source VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);