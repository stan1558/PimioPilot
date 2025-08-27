# PimioPilot
Semi-automated stock pilot with AI forecasts

## License
This project is licensed under the AGPL-3.0 License.
See the [LICENSE](LICENSE) file for the full license text.

### Third-Party Software
This project uses:

- **yfinance** (https://github.com/ranaroussi/yfinance)
- **TimescaleDB** (https://github.com/timescale/timescaledb)

---

## Requirements
- Docker
- Docker Compose

---

## Usage

### 1. Build and run services (data ingestion)
```bash
docker compose down -v
docker compose up -d --build
docker compose logs -f timescaledb
docker compose logs -f app
```

Expected output in logs:
```
{"status": "ok", "rows": 14, "out": "out/demo-001"}
```

#### Inspect artifacts
Outputs are written under `./out/demo-001/`:
- `summary.json` → task metadata (symbols, rows, cols, runtime, etc.)
- `logs.ndjson` → process logs (one JSON per line)
- `data.csv` (optional) → human-readable table

Example check:
```bash
ls -la out/demo-001
cat out/demo-001/summary.json
head -20 out/demo-001/data.csv
```

---

### 2. Run query service (data retrieval)
The query module allows other components (e.g. strategy modules) to fetch data from the DB.

```bash
docker compose rm -f query
docker compose --profile query up query --abort-on-container-exit
docker compose logs -f query
```

Expected output:
```
{"status": "ok", "rows": 14, "result": "out/queries/q_2330_20250801_20250822.csv", "out": "out/queries"}
```

#### Query artifacts
Outputs are written under `./out/queries/`:
- `*.csv` → result table
- `*.log.ndjson` → query logs
- `*.manifest.json` → query metadata (config, rows, artifacts)

Example check:
```bash
cat out/queries/q_2330_20250801_20250822.manifest.json
```

---

## License & Credits
- [yfinance](https://github.com/ranaroussi/yfinance) (Apache 2.0 License)
- [TimescaleDB](https://github.com/timescale/timescaledb) (Apache 2.0 License + Timescale License for advanced features)
- [pandas](https://github.com/pandas-dev/pandas) (BSD 3-Clause License)
- [psycopg2-binary](https://github.com/psycopg/psycopg2) (LGPL License)
- Project code licensed under [MIT License](./LICENSE)
