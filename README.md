# Admin Docs Pack

> Drop these into your repo as individual files. I’ve included placeholders for secrets — do **not** commit real keys. This pack replaces the old `README.md` with a production‑ready set of ops docs.

---

## README.md

# Whale Discovery Scanner

A lightweight service that scans on‑chain and exchange data to discover potential whale activity and writes results to a PostgreSQL database. Built for deployment on Render.

## Features

* Scheduled scanning of on‑chain/exchange sources
* Writes normalised events to PostgreSQL
* Configured entirely via environment variables in Render
* Minimal dependency footprint

## Tech stack

* Python 3.11
* Requests for HTTP
* Psycopg 3 (binary) for PostgreSQL

## Quick start

### 1) Prerequisites

* Python 3.11+
* PostgreSQL 14+ (local or hosted)
* A `.env` file (see **.env.example** below)

### 2) Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 3) Environment

Create `.env` from `.env.example` and fill in your values (never commit real secrets):

```bash
cp .env.example .env
```

### 4) Run locally

```bash
export $(grep -v '^#' .env | xargs)  # or use direnv
python whale-discovery-scanner.py
```

### 5) Deploy (Render)

* Create a **Web Service** on Render targeting this repo
* Add **Environment Variables** from this README’s **Configuration** section
* Choose Python runtime `3.11`
* Build command: `pip install -r requirements.txt`
* Start command: `python whale-discovery-scanner.py`

### 6) Health

* Logs: Render → *Logs*
* Metrics: Render → *Metrics*

## Configuration (Env Vars)

See **.env.example** for the authoritative list and descriptions. Configure these in Render → *Environment*.

## Safety

* Never commit secrets. Use Render’s encrypted env vars.
* Rotate keys immediately if exposed.

---

## RUNBOOK.md

# Operations Runbook

**Audience:** On‑call engineers and operators.

## 1) Routine operations

* **Check service health:** Render → *Logs* for errors in last 24h, *Metrics* for CPU/memory spikes.
* **Rotate cache bust flag:** Update `CACHE_BUST_FINAL` to a new value to force a cold start on next deploy.
* **Database checks:**

  * Connectivity: `psql $TRINITY_DATABASE_URL -c "select now();"`
  * Size & locks: `\l+`, `select * from pg_locks where not granted;`

## 2) Common incidents & playbooks

### A) Service won’t start

**Symptoms:** Crash on boot, Render shows repeated restarts.
**Actions:**

1. Check last 300 log lines.
2. Verify Python version is 3.11.
3. Confirm required env vars exist and have non‑empty values.
4. Run locally with `.env` to reproduce.
5. Redeploy after fixing.

### B) Database connection failures

**Symptoms:** `connection refused` / `auth failed` / TLS issues.
**Actions:**

1. Confirm `TRINITY_DATABASE_URL` is valid (host, db, user, password, port 5432).
2. Confirm network egress from Render to your DB host is allowed.
3. Regenerate password; update env var; redeploy.
4. Test with: `psql $TRINITY_DATABASE_URL -c "select 1;"`

### C) API quota exhausted / 401

**Actions:**

1. Validate the relevant `*_API_KEY` exists and is current.
2. Regenerate key in provider dashboard; rotate in Render; redeploy.

### D) Spikes in errors after deploy

**Actions:**

1. Roll back to previous successful build in Render *Builds*.
2. Compare diff; revert or hot‑fix; redeploy.

## 3) Backup & restore

* **Backups:** Enable automated daily snapshots on your PostgreSQL host.
* **Manual dump:** `pg_dump --no-owner --format=custom "$TRINITY_DATABASE_URL" -f backup/$(date +%F).dump`
* **Restore:** `pg_restore --clean --if-exists --no-owner -d "$TRINITY_DATABASE_URL" backup/DATE.dump`

## 4) Key rotation procedure

1. Create new key in provider.
2. Add new env var in Render.
3. Click **Deploy latest commit** to apply without code changes.
4. Delete old key from provider when stable.

## 5) Change management

* Use PRs; require 1 approval.
* Tag releases `vX.Y.Z`.
* Update **CHANGELOG.md** per release.

---

## .env.example

```dotenv
# Application
CACHE_BUST_FINAL=v6-rebuild-YYYY-MM-DD-HHMM

# Data sources
SOLSCAN_API_KEY=replace-me
BLOCKCYPHER_API_KEY=replace-me
COINGECKO_API_KEY=replace-me
ETHERSCAN_API_KEY=replace-me
KRAKEN_API_KEY=replace-me
KRAKEN_PRIVATE_KEY=replace-me
SOLANA_WHALE_ADDRESSES=addr1,addr2 # comma-separated

# Database (PostgreSQL)
# Format: postgresql://USER:PASSWORD@HOST:5432/DBNAME
TRINITY_DATABASE_URL=postgresql://wallet_admin:password@host:5432/wallet_transactions
```

> Store these in Render → *Environment*. Do **not** commit a real `.env`.

---

## SECURITY.md

# Security Policy

## Supported versions

Security patches follow main branch releases for 6 months.

## Reporting a vulnerability

Email the maintainers (security contact for your org). We aim to triage within 48 hours.

## Handling secrets

* All API keys and DB credentials live in Render Env Vars.
* Never echo secrets in logs.
* Rotate keys at least quarterly or immediately on suspicion of compromise.

## Data protection

* Only store minimally necessary data.
* Use TLS to external APIs and DB.
* Restrict DB roles to **least privilege**.

---

## DEPLOYMENT.md

# Deployment Guide (Render)

1. **Create Service** → Web Service → connect GitHub repo.
2. **Environment** → add vars from `.env.example`.
3. **Build**: `pip install -r requirements.txt`
4. **Start**: `python whale-discovery-scanner.py`
5. **Autoscaling**: start with 1x; review CPU/memory after traffic.
6. **Rollbacks**: Use *Builds* → revert to last green build.

---

## DATABASE.md

# Database Guide

### URL format

`postgresql://USER:PASSWORD@HOST:5432/DBNAME`

### Connectivity checks

```bash
psql "$TRINITY_DATABASE_URL" -c "select version();"
```

### Migrations

* If you add schema changes, create migration scripts in `db/migrations/` and apply with `psql -f` or a migration tool of your choice.

### Maintenance

* `VACUUM (ANALYZE);` weekly on write‑heavy tables.
* Monitor connection count and slow queries.

---

## MONITORING.md

# Monitoring & Alerting

* **Logs:** Render → *Logs* with retention per plan
* **Metrics:** CPU, memory, restarts
* **External checks:** Optional: UptimeRobot/Healthchecks crons hitting an internal `/health` endpoint (add if needed)

Alert thresholds (suggested):

* > 2 restarts in 10 minutes → page
* Error rate >2% over 5 min → investigate

---

## ARCHITECTURE.md

# Architecture Overview

* **whale-discovery-scanner.py** – main runner: pulls data from configured sources, normalises, writes to DB.
* **debug.py / test.py** – local helpers for troubleshooting and validation.
* **PostgreSQL** – persistence layer for events and state.

**Flow:** Providers → HTTP (API Keys) → Scanner → Normalised events → PostgreSQL.

**Config:** Everything via env vars; zero hard‑coded secrets.

---

## CONTRIBUTING.md

* Create a feature branch, open a PR with a clear description and testing notes.
* Run linters/tests locally before opening PR.
* Keep docs updated, especially `.env.example` and **RUNBOOK.md**.

---

## CODE\_OF\_CONDUCT.md

We commit to a friendly, safe, and professional environment. Be respectful; no harassment or discrimination. Report issues to maintainers.

---

## LICENSE.md

Copyright © 2025 Your Organisation. All rights reserved.

> Replace with an OSI licence if you intend to open‑source (MIT/Apache‑2.0).

---

## CHANGELOG.md

All notable changes will be documented here following **Keep a Changelog** and **SemVer**.

* `v0.1.0` – Initial public docs pack.

---

## SUPPORT.md

* Issues: GitHub Issues in this repo
* Emergencies: On‑call via your organisation’s escalation policy

---

## TESTING.md

* Unit tests: add under `tests/`
* Integration: use a staging DB URL and sandbox API keys
* Local smoke test: run scanner for 5 minutes and confirm DB writes

---

## MAINTAINERS.md

List primary and backup maintainers with contact routes (email, Slack, phone). Omit from public repos if sensitive.
