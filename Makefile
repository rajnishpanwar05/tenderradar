.PHONY: help install dev-install pre-commit test lint security docker-build docker-up docker-down clean

help:
	@echo ""
	@echo "TenderRadar — Available Commands"
	@echo "================================="
	@echo "  make install       Install production dependencies"
	@echo "  make dev-install   Install dev + test dependencies"
	@echo "  make pre-commit    Install pre-commit hooks"
	@echo "  make test          Run test suite"
	@echo "  make lint          Run ruff linter"
	@echo "  make security      Run bandit security scan"
	@echo "  make docker-build  Build Docker image"
	@echo "  make docker-up     Start all services (docker-compose)"
	@echo "  make docker-down   Stop all services"
	@echo "  make clean         Remove cache files"
	@echo ""

install:
	pip install -r requirements.txt

dev-install:
	pip install -r requirements.txt
	pip install pytest pytest-cov ruff black isort bandit pre-commit

pre-commit:
	pip install pre-commit
	pre-commit install
	@echo "Pre-commit hooks installed."

test:
	pytest tests/ -v --tb=short

lint:
	ruff check intelligence/ database/ api/ pipeline/ scrapers/

security:
	bandit -r intelligence/ database/ api/ pipeline/ --skip B101

docker-build:
	docker build -t tenderradar:local .

docker-up:
	docker-compose up -d

docker-down:
	docker-compose down

clean:
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type d -name ".ruff_cache" -exec rm -rf {} +
