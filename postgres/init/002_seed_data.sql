INSERT INTO companies (ticker, company_name, sector, industry, exchange)
VALUES
    ('AAPL', 'Apple Inc.', 'Technology', 'Consumer Electronics', 'NASDAQ'),
    ('MSFT', 'Microsoft Corp.', 'Technology', 'Software', 'NASDAQ'),
    ('TSLA', 'Tesla Inc.', 'Automotive', 'Electric Vehicles', 'NASDAQ'),
    ('AMZN', 'Amazon.com Inc.', 'Consumer Discretionary', 'E-Commerce', 'NASDAQ'),
    ('GOOGL', 'Alphabet Inc.', 'Technology', 'Internet Services', 'NASDAQ')
ON CONFLICT (ticker) DO NOTHING;