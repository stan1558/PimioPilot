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

### 1. Build and run services
```bash
docker compose down -v
docker compose up -d --build
docker compose logs -f app   # follow logs
```

Expected output in logs:
```
{"status": "ok", "rows": 14, "out": "out/demo-001"}
```

### 2. Inspect artifacts
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

## Configuration

Job configs are written in **YAML** and validated with **JSON Schema**.

- Schema: [`schemas/job.schema.json`](schemas/job.schema.json)
- Example job: [`examples/job.yaml`](examples/job.yaml)

### Example `examples/job.yaml`
```yaml
task_id: demo-001
source: yfinance
symbols: ["2330.TW"]
interval: "1d"
range:
  start: "2025-08-01"
  end: "2025-08-22"

yfinance_options:
  auto_adjust: true
  actions: false
  prepost: false
  threads: auto

outputs:
  out_dir: "./out/demo-001"
  write_csv: true
  csv_filename: "data.csv"
```

---

## CLI (inside container)

You can also run jobs manually inside the app container:

```bash
docker compose exec app \
  python -m pimiopilot_data.cli run --config examples/job.yaml
```

---

## Next Steps

- **TimescaleDB Integration**:
  The sink is stubbed in [`src/pimiopilot_data/sinks/timescaledb_stub.py`](src/pimiopilot_data/sinks/timescaledb_stub.py).
  Future work: implement bulk upsert (COPY + ON CONFLICT) with schema:

  ```
  table: tw_ticks
  primary key: (symbol, ts)
  columns: symbol, ts, open, high, low, close, adj_close, volume, src_interval
  ```

---

## Notes

- Old test scripts (`fetch_taiwan_stock_yfinance.py`, `query_timescaledb.py`) are moved to `legacy_scripts/` for reference.
- Current pipeline does **not** write into TimescaleDB yet — only fetches and exports to files.

---

## License & Credits
- [yfinance](https://github.com/ranaroussi/yfinance) (Apache 2.0 License)
- [TimescaleDB](https://github.com/timescale/timescaledb) (Apache 2.0 License + Timescale License for advanced features)
- [pandas](https://github.com/pandas-dev/pandas) (BSD 3-Clause License)
- [psycopg2-binary](https://github.com/psycopg/psycopg2) (LGPL License)
- Project code licensed under [MIT License](./LICENSE)
