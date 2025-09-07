# Data Module Architecture

This document describes the internal architecture and data flow of the **data module**.

## Overview

```mermaid
flowchart LR
  classDef store fill:#f3f9ff,stroke:#6aa6ff,stroke-width:1px
  classDef proc fill:#f8fff3,stroke:#79c26a,stroke-width:1px
  classDef io fill:#fff8f3,stroke:#ffb36a,stroke-width:1px
  classDef misc fill:#f7f7f7,stroke:#bbb,stroke-width:1px

  CFG[Config]:::misc
  SRC[(Data Sources)]:::io
  ING[Ingestion]:::proc
  VAL[Validation]:::proc
  TRF[Transform]:::proc
  DB[(Storage: TimescaleDB/Postgres)]:::store
  OUT[Outputs]:::io
  LOG[[Logging/Monitoring]]:::misc
  SCH[[Scheduler]]:::misc
  CLI[[CLI/Entry Points]]:::misc

  CFG --> ING
  CFG --> DB
  CFG --> OUT
  SRC --> ING
  ING --> VAL
  VAL --> TRF
  TRF --> DB
  DB --> OUT
  ING --> LOG
  VAL --> LOG
  TRF --> LOG
  DB --> LOG
  OUT --> LOG
  SCH --> ING
  CLI --> ING
  CLI --> OUT
```

## Components

- **Data Sources**: External providers such as yfinance.
- **Scheduler**: cron/CI triggers for periodic ingestion.
- **Config**: .env / YAML settings for credentials and parameters.
- **CLI**: Command-line interfaces or scripts to run ad-hoc tasks.
- **Ingestion**: Jobs/services that fetch raw OHLCV and metadata.
- **Validation**: Schema checks using JSON Schema / Pydantic to ensure data quality.
- **Transform**: Cleaning, normalization, feature enrichment.
- **Storage**: PostgreSQL/TimescaleDB hypertables for time-series data.
- **Outputs**: Exports (CSV), reports, and charts consumed by downstream modules.
- **Logging**: Centralized logs/metrics for observability and alerting.

## Notes

- Keep this diagram text-based (Mermaid) to ease versioning and reviews.
- Update node labels with concrete module/file names as the implementation stabilizes.
