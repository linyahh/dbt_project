# Heymax — dbt analytics project

A compact, production-minded dbt project that transforms event data into
analytical datasets for growth, engagement, and retention analysis.

This repository contains dbt models, macros, and tests used to build
layered analytical datasets (staging → core → intermediate → marts).

## Quick start

Prerequisites:

- Python and dbt (adapter matching your warehouse)
- A configured `profiles.yml` for your target environment

Basic commands:

```bash
dbt deps         # install packages
dbt seed         # load seed data (if any)
dbt run          # run models
dbt test         # run tests
dbt docs generate
dbt docs serve
```

To run only daily-refresh models (tagged `refresh_daily`):

```bash
dbt build --select tag:refresh_daily
```

If you need a backfill for incremental models, pass a var as implemented
in the project, for example:

```bash
dbt run --vars '{"backfill_start_date": "2024-01-01"}'
```

## Project layout (high level)

- `models/` — dbt models organized into `staging`, `core`, `intermediate`,
	and `marts`
- `macros/` — reusable SQL macros used across models
- `analyses/` — one-off analyses
- `tests/` — custom tests or test-related artifacts
- `target/` — compiled artifacts and run outputs (auto-generated)

Key model families in this repo include event facts (`fct_events`),
user dimension (`dim_user`), and derived growth/engagement marts.

## Development notes

- Keep transformations modular: prefer intermediate models to share
	logic across different time grains.
- Use macros to encapsulate repeated SQL patterns (period generation,
	aggregation helpers, etc.).
- Tag models that must run on a schedule (e.g., `refresh_daily`).

## Testing & quality

This project uses dbt tests for basic data quality checks (not null,
uniqueness, relationships). Run `dbt test` after building models.

## Contributing

If you'd like changes or have questions, open an issue or send a PR.
Please include a short description of the change and relevant context.

If you want help extending this README with deploy/run specifics for
your warehouse or CI, tell me which adapter and CI provider you use and
I’ll add step-by-step instructions.

---

If you'd like, I can also:

- add a short `HOWTO` for local development (virtualenv, installing dbt),
- add CI examples (GitHub Actions) for scheduled runs and tests, or
- generate a minimal `profiles.yml` template for your warehouse.

Tell me which of these you'd like next.

