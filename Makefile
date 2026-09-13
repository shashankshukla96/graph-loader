.PHONY: dev dev-down dev-logs

dev:
	@bash scripts/start_dev.sh

dev-down:
	docker compose down -v

dev-logs:
	docker compose logs -f
