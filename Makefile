# ─── Lume capstone project ──────────────────────────────────────────────────
#
# Run from the project root. The scaffold ships only the upstream:
#   * data-init   — one-shot sidecar that downloads + preprocesses the LCL
#                   dataset into a named docker volume. ~5-15 min on first run
#                   (765 MB download). Idempotent — exits in <2s after that.
#   * meter-vendor — FastAPI service that serves GET /readings and pushes
#                    POST webhooks to your subscribers, advancing through the
#                    historical window at configurable replay speed.
#
# Everything else (ingest, storage, dashboards, alerting, monitoring) is yours
# to design. Add services to compose.yml as you need them.
# ────────────────────────────────────────────────────────────────────────────

.PHONY: run stop reset logs logs-ingest logs-backfill vendor-chaos vendor-calm help

help:
	@echo ""
	@echo "  make run            Build all images, run data-init, start all services"
	@echo "  make stop           Stop containers (keeps volumes)"
	@echo "  make reset          Stop + wipe volumes (next run re-downloads 765 MB)"
	@echo "  make logs           Tail meter-vendor logs"
	@echo "  make logs-ingest    Tail ingest-gateway logs"
	@echo "  make logs-backfill  Tail backfill service logs"
	@echo "  make vendor-chaos   Restart meter-vendor with duplicates/reordering/outages on"
	@echo "  make vendor-calm    Restart meter-vendor with chaos all-zero"
	@echo ""
	@echo "  Vendor API:      http://localhost:18100/docs"
	@echo "  Vendor health:   http://localhost:18100/healthz"
	@echo "  Ingest health:   http://localhost:18080/healthz"
	@echo "  Kafka:           localhost:19092"
	@echo ""

run:
	docker compose up -d --build
	@echo ""
	@echo "=============================================================="
	@echo " Lume vendor mock is starting."
	@echo "   First run downloads ~765 MB (5-15 min). Watch progress:"
	@echo "     docker compose logs -f data-init"
	@echo "   Once meter-vendor is healthy:"
	@echo "     curl http://localhost:18100/healthz"
	@echo "     open http://localhost:18100/docs"
	@echo "=============================================================="

stop:
	docker compose down --remove-orphans

reset:
	docker compose down -v --remove-orphans

logs:
	docker compose logs -f meter-vendor

logs-ingest:
	docker compose logs -f ingest-gateway

logs-backfill:
	docker compose logs -f backfill

vendor-chaos:
	VENDOR_DUPLICATE_RATE=0.05 \
	VENDOR_REORDER_RATE=0.15 \
	VENDOR_LATE_RATE=0.02 \
	VENDOR_OUTAGE_SCHEDULE=02:15-02:20 \
	docker compose up -d --no-deps --force-recreate meter-vendor
	@echo "[chaos] meter-vendor restarted with duplicates/reordering/outages on."

vendor-calm:
	VENDOR_DUPLICATE_RATE=0.0 \
	VENDOR_REORDER_RATE=0.0 \
	VENDOR_LATE_RATE=0.0 \
	VENDOR_OUTAGE_SCHEDULE= \
	docker compose up -d --no-deps --force-recreate meter-vendor
	@echo "[calm] meter-vendor restarted with chaos disabled."
