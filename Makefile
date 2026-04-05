up:
	docker compose -f infra/docker-compose.yml up -d

down:
	docker compose -f infra/docker-compose.yml down

logs:
	docker compose -f infra/docker-compose.yml logs -f

dbt-run:
	cd dbt && dbt run

dbt-test:
	cd dbt && dbt test

test:
	pytest -q

format:
	black . && isort .

lint:
	ruff check .