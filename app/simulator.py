import time
import random
from datetime import datetime
from faker import Faker
import psycopg2
from psycopg2.extras import execute_values

fake = Faker()

SP500_TICKERS = [
    "AAPL", "MSFT", "AMZN", "NVDA", "GOOGL", "META", "TSLA", "JPM",
    "V", "XOM", "UNH", "LLY", "PG", "HD", "MA", "CRM", "NFLX", "AMD"
]

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="trading_db",
    user="postgres",
    password="postgres"
)
conn.autocommit = True
cur = conn.cursor()

def seed_accounts(n=20):
    accounts = []
    for i in range(n):
        account_id = f"ACC{str(i+1).zfill(6)}"
        accounts.append((
            account_id,
            fake.name(),
            random.choice(["RETAIL", "MARGIN", "IRA"]),
            round(random.uniform(10000, 200000), 2),
            round(random.uniform(10000, 300000), 2)
        ))

    execute_values(
        cur,
        """
        INSERT INTO trading_accounts
        (account_id, account_name, account_type, cash_balance, buying_power)
        VALUES %s
        ON CONFLICT (account_id) DO NOTHING
        """,
        accounts
    )

def create_order():
    cur.execute("SELECT account_id FROM trading_accounts ORDER BY RANDOM() LIMIT 1")
    row = cur.fetchone()
    if not row:
        return
    account_id = row[0]

    cur.execute(
        """
        INSERT INTO trade_orders
        (account_id, ticker_symbol, order_type, quantity, limit_price, status)
        VALUES (%s, %s, %s, %s, %s, 'PENDING')
        """,
        (
            account_id,
            random.choice(SP500_TICKERS),
            random.choice(["BUY", "SELL"]),
            random.randint(1, 200),
            round(random.uniform(50, 500), 2),
        ),
    )

def execute_pending_orders():
    cur.execute(
        """
        UPDATE trade_orders
        SET status = 'EXECUTED',
            executed_price = ROUND((limit_price * (1 + (RANDOM() - 0.5) * 0.02))::numeric, 2),
            executed_at = NOW()
        WHERE order_id IN (
            SELECT order_id
            FROM trade_orders
            WHERE status = 'PENDING'
            AND placed_at < NOW() - INTERVAL '10 seconds'
            ORDER BY placed_at
            LIMIT 10
        )
        """
    )

def settle_executed_orders():
    cur.execute(
        """
        UPDATE trade_orders
        SET status = 'SETTLED',
            settled_at = NOW()
        WHERE order_id IN (
            SELECT order_id
            FROM trade_orders
            WHERE status = 'EXECUTED'
            AND executed_at < NOW() - INTERVAL '20 seconds'
            ORDER BY executed_at
            LIMIT 10
        )
        """
    )

def update_positions():
    cur.execute(
        """
        INSERT INTO portfolio_positions (account_id, ticker_symbol, quantity, avg_cost_basis)
        SELECT
            account_id,
            ticker_symbol,
            SUM(CASE WHEN order_type = 'BUY' THEN quantity ELSE -quantity END) AS qty,
            AVG(COALESCE(executed_price, limit_price)) AS avg_price
        FROM trade_orders
        WHERE status = 'EXECUTED'
          AND executed_at > NOW() - INTERVAL '1 minute'
        GROUP BY account_id, ticker_symbol
        ON CONFLICT (account_id, ticker_symbol)
        DO UPDATE SET
            quantity = portfolio_positions.quantity + EXCLUDED.quantity,
            avg_cost_basis = EXCLUDED.avg_cost_basis,
            updated_at = NOW()
        """
    )

def main():
    seed_accounts()
    print("Starting trading simulator...")
    cycle = 0

    while True:
        cycle += 1
        for _ in range(random.randint(2, 6)):
            create_order()

        execute_pending_orders()
        settle_executed_orders()
        update_positions()

        print(f"Cycle {cycle} at {datetime.now().strftime('%H:%M:%S')}")
        time.sleep(5)

if __name__ == "__main__":
    main()
