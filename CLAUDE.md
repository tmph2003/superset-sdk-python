---
name: tool-sync-dbt-metric-to-superset
description: >
  Synchronizes dbt semantic layer (MetricFlow) with Apache Superset by generating,
  transforming, and managing metrics, datasets, and their relationships.

  Use this tool when working with dbt semantic models and Superset integration,
  including tasks such as:
  - Generating metrics from MetricFlow
  - Mapping dimensions, measures, and joins into datasets
  - Creating or updating Superset datasets and metrics via API
  - Ensuring grain consistency and correct key relationships
  - Validating, debugging, and adjusting metric definitions
  - Handling auxiliary data modeling and synchronization tasks
compatibility: >
  Requires access to a dbt project with semantic models (MetricFlow) and a running
  Apache Superset instance with API access. Assumes knowledge of data modeling,
  including facts, dimensions, joins, and metric definitions.
---

## Overview
This tool synchronizes dbt semantic models (MetricFlow) with Apache Superset.

It is responsible for generating, transforming, validating, and syncing metrics,
datasets, and relationships from dbt into Superset.

---

## When to Use
Use this tool when:
- You need to expose dbt metrics in Superset
- You need to create or update Superset datasets/metrics
- You are debugging incorrect metrics, joins, or aggregations
- You are validating grain, keys, or relationships between models

---

## Core Responsibilities

1. Parse dbt semantic layer (facts, dimensions, measures, metrics)
2. Generate metrics via MetricFlow
3. Map dbt structures into Superset datasets
4. Handle joins between fact and dimension tables
5. Ensure correct grain and key relationships
6. Sync datasets and metrics to Superset via API
7. Validate and debug metric definitions and queries

---

## Step-by-Step Instructions

### 1. Identify Relevant Models
- Locate fact and dimension models from dbt semantic layer
- Determine primary keys and join keys
- Identify grain of each model

### 2. Validate Relationships
- Ensure joins are valid (1-1, many-1)
- Avoid many-many joins unless explicitly handled
- Confirm no grain conflicts

### 3. Generate Metrics
- Use MetricFlow to compute metrics
- Ensure metrics align with model grain
- Validate aggregations (SUM, COUNT, DISTINCT, etc.)

### 4. Build Superset Dataset
- Combine fact and dimension tables if needed
- Ensure all required columns exist
- Maintain consistent naming

### 5. Sync to Superset
- Create or update dataset via API
- Create or update metrics
- Ensure idempotency (safe to re-run)

### 6. Validate Output
- Check metric correctness
- Validate joins and filters
- Debug if results are inconsistent

---

## Examples

### Example 1: Simple Metric Sync

**Input**
- fact_orders(user_id, order_id, revenue)
- metric: total_revenue = SUM(revenue)

**Output**
- Superset dataset: fact_orders
- Metric: total_revenue

---

### Example 2: Join Fact and Dimension

**Input**
- fact_orders(user_id, revenue)
- dim_users(user_id, country)

**Output**
- Dataset with join on user_id
- Metric grouped by country

---

## Common Edge Cases

### 1. Grain Mismatch
- Fact at order level, dimension at user level
- Ensure aggregation happens before join if needed

### 2. Missing Keys
- Join key not present in one table
- Must not attempt invalid joins

### 3. Many-to-Many Joins
- Avoid unless explicitly handled via bridge tables

### 4. Duplicate Metrics
- Ensure unique naming in Superset

### 5. Schema Drift
- dbt model changes but Superset not updated
- Must support update flow

---

## Guidelines

- Always prioritize correct grain and join logic over convenience
- Do not assume joins are valid without validation
- Prefer explicit over implicit relationships
- Ensure all operations are idempotent
- Keep naming consistent and predictable

---

## Debugging Strategy

When metrics are incorrect:
1. Check grain of each model
2. Validate join conditions
3. Inspect aggregation logic
4. Compare raw vs aggregated data
5. Recompute metric via MetricFlow

---

## Notes

- This tool operates at the semantic layer level, not raw SQL only
- Superset datasets must reflect correct data modeling principles
- Incorrect joins or grain mismatches will lead to wrong metrics