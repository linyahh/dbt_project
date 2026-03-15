
# AI Agent Design: dbt Column-Level Documentation Assistant

## Overview

As **dbt projects scale**, maintaining accurate and consistent **column-level documentation** becomes increasingly difficult. New columns are frequently added while existing transformations evolve, but documentation often lags behind. This results in:

- incomplete dbt documentation
- reduced data discoverability
- lower trust in the analytics layer

To address this problem, I propose an **AI-powered documentation agent** that automatically **proposes column descriptions** for dbt models as they evolve.

The agent integrates into the **Pull Request workflow** and generates documentation suggestions for **new or modified columns**. Importantly, the agent **never writes directly to production documentation**. Instead, it proposes updates that require **human review and approval**, ensuring accuracy and preserving trust in the analytics layer.

---

# A. Agent Architecture

## System Execution Flow

```
                ┌────────────────────────────┐
                │        GitHub PR           │
                │  (SQL / schema.yml change) │
                └─────────────┬──────────────┘
                              │
                              ▼
                ┌────────────────────────────┐
                │     GitHub Actions CI      │
                │  Trigger agent workflow    │
                └─────────────┬──────────────┘
                              │
                              ▼
                ┌────────────────────────────┐
                │       GitHub API           │
                │   Extract PR diff          │
                │   Detect changed files     │
                └─────────────┬──────────────┘
                              │
                              ▼
                ┌────────────────────────────┐
                │   dbt Metadata Retrieval   │
                │   manifest.json + schema   │
                │   lineage + existing docs  │
                └─────────────┬──────────────┘
                              │
                              ▼
                ┌────────────────────────────┐
                │        SQL Parser          │
                │  Extract columns + logic   │
                │  (sqlglot / compiled SQL)  │
                └─────────────┬──────────────┘
                              │
                              ▼
                ┌────────────────────────────┐
                │       Context Builder      │
                │ SQL + upstream docs +      │
                │ existing descriptions      │
                └─────────────┬──────────────┘
                              │
                              ▼
                ┌────────────────────────────┐
                │          LLM API           │
                │ Generate column docs       │
                │ (OpenAI / model provider)  │
                └─────────────┬──────────────┘
                              │
                              ▼
                ┌────────────────────────────┐
                │       Validation Layer     │
                │ Generic text checks        │
                │ SQL consistency checks     │
                │ Formatting checks          │
                └─────────────┬──────────────┘
                              │
                              ▼
                ┌────────────────────────────┐
                │    YAML Patch Generator    │
                │ Update schema.yml diff     │
                └─────────────┬──────────────┘
                              │
                              ▼
                ┌────────────────────────────┐
                │      GitHub PR Comment     │
                │ Suggested documentation    │
                │ Human review required      │
                └─────────────┬──────────────┘
                              │
                              ▼
                ┌────────────────────────────┐
                │       Merge + Publish      │
                │ Approved docs committed    │
                └────────────────────────────┘
```

---

# Flow + Tools + Failure Points

| Flow Step | Tool / API | Why It Is Needed | Example Failure Point |
|---|---|---|---|
| **Trigger on PR update** | GitHub Actions | Start the agent automatically when dbt files change | Agent not triggered for some PR events |
| **Extract changed files and diff** | GitHub API | Detect modified dbt models and documentation | Renamed or moved files not detected |
| **Read project metadata** | dbt `manifest.json`, `schema.yml` | Retrieve lineage and existing documentation | Manifest outdated or missing metadata |
| **Parse SQL columns** | SQL parser (`sqlglot`, compiled dbt SQL) | Extract column names and transformation logic | Complex SQL or macros not parsed correctly |
| **Build prompt context** | Context builder service | Combine SQL logic, upstream docs, metadata | Missing or incorrect context passed to model |
| **Generate descriptions** | LLM API | Produce column-level documentation drafts | Hallucinated or vague descriptions |
| **Validate outputs** | Rule-based validation layer | Catch generic or incorrect documentation | Weak validation allows bad docs through |
| **Surface changes** | GitHub PR comments / suggested diff | Allow reviewers to inspect and approve docs | Diff too noisy or unclear |
| **Monitor system health** | Logging, metrics, dashboards | Detect performance or quality degradation | Silent drop in documentation quality |

---

# B. Human-in-the-Loop Design

Human review is required whenever the agent proposes documentation updates.

Reviewers inspect **AI-generated descriptions directly in the Pull Request** before merging.

To prevent the agent from **silently introducing incorrect documentation**:

- The agent **never writes directly to the main branch**
- All updates appear as **PR suggestions**
- Validation filters low-quality outputs
- Low-confidence descriptions are flagged

Proposed changes are surfaced through:

- **GitHub PR comments**
- **YAML diffs**
- Optional **Slack notifications**

This ensures documentation changes remain **visible, reviewable, and auditable**.

---

# C. Failure Modes & Observability

### 1. Hallucinated or Incorrect Documentation

The LLM may generate plausible descriptions that **do not match the SQL logic**.

Detection:

- reviewer rejection rate
- edit distance between generated and approved descriptions
- benchmark evaluation sets

---

### 2. Stale Documentation

The agent may fail to detect column changes, causing documentation drift.

Detection:

- compare SQL columns with `schema.yml`
- track **documentation coverage metrics**

---

### 3. Generic or Low-Quality Documentation

Descriptions may be vague and not helpful.

Detection:

- heuristic checks
- reviewer feedback signals
- monitoring repeated generic phrases

---

### Observability

The system logs:

- prompt versions
- generated outputs
- validation results
- reviewer decisions

Dashboards track:

- documentation coverage
- acceptance rate
- SQL parsing failures

Alerts trigger when:

- coverage drops
- rejection rate spikes

---

# D. Scope & Build Plan

A **one-week V1** would implement a minimal but functional system:

### In Scope

- PR-triggered documentation agent
- dbt model detection
- SQL column extraction
- LLM-based documentation generation
- rule-based validation
- PR-based review workflow

### Out of Scope

- deep semantic SQL reasoning
- automatic documentation merges
- enterprise glossary integration
- full warehouse backfill

### Success Metrics

The agent will be considered successful if it:

- increases **documentation coverage**
- achieves **high reviewer acceptance rates**
- reduces **manual documentation effort**

---

# Appendix

## LLM Prompt Template — Generation

### Persona

You are an **analytics engineer assisting with dbt documentation**.

Your job is to generate **accurate and concise column descriptions** based strictly on SQL logic and metadata.

### Task

Generate a **clear column-level description**.

### Context

Model: `int_user_activity`  
Column: `lifetime_events`

SQL logic:

```
count(distinct event_ts) as lifetime_events
```

Upstream description:

```
event_ts: timestamp when the user performed an event
```

Existing documentation:

```
None
```

### Guidelines

- Use only the provided SQL and metadata
- Do not invent business meaning
- Keep the description concise (1 sentence)
- Avoid generic wording

### Output Format

```json
{
  "column_name": "lifetime_events",
  "description": "<generated description>"
}
```

---

# LLM Prompt Template — Validation

### Persona

You are a **strict reviewer for dbt column documentation**.

### Task

Evaluate whether the generated description accurately reflects the SQL logic.

### Context

Model: `int_user_activity`  
Column: `lifetime_events`

SQL:

```
count(distinct event_ts) as lifetime_events
```

Generated description:

```
Total number of distinct events recorded for the user.
```

---

### Validation Rules

- Must match SQL transformation
- Must not introduce unsupported meaning
- Must not be vague
- Must be concise (1 sentence)

---

### Output

If valid:

```json
{
  "valid": true,
  "issues": [],
  "confidence": "high"
}
```

If invalid:

```json
{
  "valid": false,
  "issues": ["description does not reflect SQL logic"],
  "confidence": "low"
}
```
