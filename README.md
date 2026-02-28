# Way4 <-> VISA Reconciliation MVP Prototype

Functional MVP based on the provided technical specification:
- T+1 ingestion for Way4 and VISA datasets
- Canonical transaction model
- Matching engine: exact -> ARN -> fuzzy -> one-to-many (partial)
- Exception workflow (assign/comment/status/close)
- RBAC + immutable audit events
- Hardcoded analytics endpoint (without flexible BI)

## Stack
- Python 3 standard library only (`sqlite3`, `http.server`)
- No external dependencies required

## Docker Deploy
Build image:
```bash
cd /Users/gleb-imac/Documents/AltynBank
docker build -t way4-visa-recon:latest .
```

Run container:
```bash
docker run --name way4-visa-recon \
  -p 8080:8080 \
  -e APP_ENV=production \
  -e PAN_HASH_SECRET='replace-with-strong-random-secret' \
  -v "$(pwd)/data:/app/data" \
  -d way4-visa-recon:latest
```

Check container health:
```bash
docker ps --filter name=way4-visa-recon
docker inspect --format='{{.State.Health.Status}}' way4-visa-recon
curl http://127.0.0.1:8080/health
```

Stop/remove:
```bash
docker stop way4-visa-recon
docker rm way4-visa-recon
```

## Run
```bash
cd /Users/gleb-imac/Documents/AltynBank
export PAN_HASH_SECRET='dev-local-secret'
python3 -m src.demo_data
python3 -m src.server
```

Browser UI URL: `http://127.0.0.1:8080/`
API base URL: `http://127.0.0.1:8080/api/v1`

Health check:
```bash
curl http://127.0.0.1:8080/health
```

## Browser flow
1. Open `http://127.0.0.1:8080/`.
2. Select `Business date`.
3. `Загрузка` tab: upload `.xlsx` in one of supported profiles:
   - `AUTO_DETECT` (recommended)
   - `WAY4_1552_V1` (e.g. `Копия 1552.xlsx`)
   - `VISA_MSPK_V1` (e.g. `Копия Виза МСПК.xlsx`)
4. Click `Загрузить и запустить матчинг`.
5. Open `Результаты` to use filters/status/search, pagination and exports.
6. You can drag-and-drop `.xlsx` files onto the Ingestion drop zone; progress is shown per file.

Default users via `X-User` header:
- `admin`
- `operator1`
- `supervisor`
- `auditor`
- `finance`

## Key API endpoints
- `POST /api/v1/ingest/files`
- `POST /api/v1/ingest/xlsx`
- `POST /api/v1/ingest/xlsx/batch`
- `POST /api/v1/validate/xlsx`
- `POST /api/v1/quick-compare`
- `GET /api/v1/ingest/files/{id}/status`
- `POST /api/v1/match/runs`
- `GET /api/v1/match/runs`
- `GET /api/v1/match/status?business_date=YYYY-MM-DD`
- `GET /api/v1/match/runs/{id}`
- `GET /api/v1/results/run/{run_id}?page=1&page_size=50&status=&q=&amount_min=&amount_max=&currency=`
- `GET /api/v1/results/latest?business_date=YYYY-MM-DD&page=1&page_size=50&status=&q=&amount_min=&amount_max=&currency=&sort_by=txn_time|delta|match_score&sort_dir=asc|desc`
- `GET /api/v1/results/details/{row_id}`
- `GET /api/v1/runs/{run_id}/export.xlsx`
- `GET /api/v1/runs/{run_id}/unmatched_way4.csv`
- `GET /api/v1/runs/{run_id}/unmatched_visa.csv`
- `GET /api/v1/runs/{run_id}/mismatches_partial.xlsx`
- `GET /api/v1/exceptions`
- `GET /api/v1/exceptions/{id}`
- `POST /api/v1/exceptions/{id}/actions`
- `GET|PUT /api/v1/admin/rulesets`
- `GET /api/v1/audit/events`
- `GET /api/v1/meta/users`
- `GET /api/v1/analytics/hardcoded?business_date=YYYY-MM-DD`
- `GET /api/v1/monitor/source-balance?business_date=YYYY-MM-DD`
- `GET /api/v1/export/unmatched.csv?business_date=YYYY-MM-DD`

## Quick Compare API (without UI tab)
- `POST /api/v1/quick-compare` is kept for API compatibility and scripted integrations.
- Web UI no longer exposes the old `Загрузка Lite` screen.

API example:
```bash
WAY4_B64=$(base64 < "/Users/gleb-imac/Downloads/Копия 1552.xlsx")
VISA_B64=$(base64 < "/Users/gleb-imac/Downloads/Копия Виза МСПК.xlsx")
curl -X POST http://127.0.0.1:8080/api/v1/quick-compare \
  -H 'Content-Type: application/json' \
  -H 'X-User: admin' \
  -d "{\"business_date\":\"2025-12-15\",\"way4_file\":{\"file_name\":\"Копия 1552.xlsx\",\"file_base64\":\"${WAY4_B64}\"},\"visa_files\":[{\"file_name\":\"Копия Виза МСПК.xlsx\",\"file_base64\":\"${VISA_B64}\"}]}"
```

## Example
```bash
curl -X POST http://127.0.0.1:8080/api/v1/match/runs \
  -H 'Content-Type: application/json' \
  -H 'X-User: admin' \
  -d '{"business_date":"2026-02-22","scope_filter":"ALL"}'
```

XLSX upload example:
```bash
FILE_B64=$(base64 < "/Users/gleb-imac/Downloads/Копия 1552.xlsx")
curl -X POST http://127.0.0.1:8080/api/v1/ingest/xlsx \
  -H 'Content-Type: application/json' \
  -H 'X-User: admin' \
  -d "{\"file_name\":\"Копия 1552.xlsx\",\"file_base64\":\"${FILE_B64}\",\"parser_profile\":\"WAY4_1552_V1\",\"business_date\":\"2025-12-12\"}"
```

## Tests
```bash
cd /Users/gleb-imac/Documents/AltynBank
python3 -m unittest discover -s tests -v
```

UI e2e smoke (optional, Playwright):
```bash
python3 -m pip install playwright
python3 -m playwright install chromium
python3 -m unittest tests.test_ui_e2e_playwright -v
```
