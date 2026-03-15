# Heymax — dbt analytics project

A compact, production-minded dbt project that transforms event data into
analytical datasets for growth, engagement, and retention analysis.

The implementation highlights key analytics engineering practices
including:

-   Layered warehouse modeling built on BigQuery
-   Reusable dbt macros
-   Incremental pipelines with backfill capability
-   Daily refresh orchestration
-   Data quality testing

The project is designed to simulate a **production-ready data
transformation pipeline**.

------------------------------------------------------------------------

# Architecture Overview

The warehouse follows a **layered data modeling architecture** to
separate responsibilities and ensure maintainability.

RAW SOURCE\
event_stream_raw\
↓\
STAGING\
stg_event_stream\
↓\
CORE\
fct_events\
dim_user\
↓\
INTERMEDIATE\
int_user_activity_daily\
int_user_activity_weekly\
int_user_activity_monthly\
↓\
MARTS\
fct_growth_accounting\
fct_engagement_metrics\
fct_user_features\
↓\
REPORTING\
rpt_growth_reporting

## Layer Responsibilities

### Staging

Purpose: - Clean raw data - Standardize column names and types - Apply
minimal transformations

Materialization: **view**

Example model: stg_event_stream

------------------------------------------------------------------------

### Core

Purpose: - Define canonical business entities - Build fact and dimension
tables

Materialization: **table**

Example models: fct_events\
dim_user

------------------------------------------------------------------------

### Intermediate

Purpose: - Reusable transformations - User activity aggregation - Growth
classification logic

Example models: int_user_activity_daily\
int_user_activity_weekly\
int_user_activity_monthly

------------------------------------------------------------------------

### Marts

Purpose: - Business-facing analytical datasets - Growth and engagement
metrics

Example models: fct_growth_accounting\
fct_engagement_metrics\
fct_user_features

------------------------------------------------------------------------

### Reporting

Purpose: - Dashboard-ready datasets - Aggregated views for flexible
slicing

Example model: rpt_growth_reporting

------------------------------------------------------------------------

# dbt Macros

Macros are used extensively to reduce duplicated SQL and improve
maintainability.

## Period Generation Macro

A macro generates **daily, weekly, and monthly models** from a single
template.

Example:

{{ generate_user_activity('day') }}\
{{ generate_user_activity('week') }}\
{{ generate_user_activity('month') }}

This allows the same transformation logic to be reused across multiple
time grains.

------------------------------------------------------------------------

## General Aggregation Macro

A reusable aggregation macro enables flexible reporting queries.

Users can dynamically specify dimensions such as:

-   country
-   gender
-   utm_source
-   event_type
-   transaction_category

The macro automatically builds grouped aggregations and supports
multiple dimension combinations.

------------------------------------------------------------------------

# Incremental Processing and Backfill

The `fct_events` model is implemented as an **incremental table**.

Key features:

-   Partitioned by `event_date`
-   Incremental merge strategy
-   Processes only newly arriving events during daily runs

A **backfill parameter** allows historical reprocessing when needed.

Example:

dbt run --vars '{"backfill_start_date": "2024-01-01"}'

This allows rebuilding historical data from a specified date without
performing a full refresh.

------------------------------------------------------------------------

# Daily Refresh Pipeline

Models that require daily updates are tagged with:

tags: \['refresh_daily'\]

Example execution command:

dbt build --select tag:refresh_daily

Typical pipeline flow:

Scheduler\
↓\
dbt build --select tag:refresh_daily\
↓\
incremental fact refresh\
↓\
intermediate transformations\
↓\
mart updates\
↓\
reporting tables

------------------------------------------------------------------------

# Data Quality Testing

Basic data quality assertions are implemented using **dbt tests**.

## Not Null Tests

Ensures required fields are always present.

Examples: - event_key - user_id - event_ts

------------------------------------------------------------------------

## Uniqueness Tests

Ensures primary keys are unique.

Examples: - dim_user.user_id - fct_events.event_key

------------------------------------------------------------------------

## Relationship Tests

Ensures referential integrity between tables.

Example:

fct_events.user_id → dim_user.user_id

This prevents orphan records and enforces warehouse consistency.

------------------------------------------------------------------------

# Running the Project

Install dependencies:

dbt deps

Run the full pipeline:

dbt build

Run only daily refresh models:

dbt build --select tag:refresh_daily

Run tests:

dbt test

------------------------------------------------------------------------

# Summary

This project demonstrates how dbt can be used to build a **scalable
analytics transformation pipeline** with:

-   layered warehouse design
-   reusable SQL through macros
-   incremental processing and backfill capability
-   scheduled refresh patterns
-   built-in data quality validation

The implementation focuses on **maintainability, modularity, and
reliability**, reflecting common practices in modern analytics
engineering teams.
