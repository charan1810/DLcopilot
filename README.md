# DataCopilot

DataCopilot is an AI-powered, full-stack data intelligence platform that gives organizations a unified, self-service interface to connect, explore, query, and manage their databases — across multiple environments and database engines — without requiring deep technical expertise.

It combines **schema exploration**, **natural language querying**, **data lineage mapping**, **environment comparison**, and **pipeline orchestration** into a single web application, backed by an AI layer (OpenAI GPT) that translates business questions into SQL, explains data structures, and automates repetitive engineering tasks — all secured by role-based access control.

---

## Why DataCopilot Was Built

Today, end-to-end data work requires multiple teams, multiple tools, and weeks of back-and-forth — before a business user gets a single answer.

![Current State – Manual Multi-Tool Process](docs/images/current-state.png)

The current landscape forces organizations to coordinate across a Database/Cloud Developer (Snowflake, Databricks, AWS), an ETL Developer (Informatica, Azure Data Factory, Talend), a Data Analyst (SQL, Excel, Jupyter), and a BI Developer (Power BI, Tableau, Looker) — just to deliver one report. Each handoff adds days of delay, inconsistency, and cost. When something changes, everyone is impacted and no one has full visibility.

---

## The Future State — One Platform for the Entire Data Lifecycle

DataCopilot replaces that fragmented chain with a single, unified, AI-powered platform.

![Future State – Unified AI-Powered Platform](docs/images/future-state.png)

From connecting to a data source all the way to delivering business insights — explore, transform, validate, track lineage, and query in natural language — all in one place, with no tool switching and no external dependencies.

---

## How DataCopilot Helps Users

![How DataCopilot Helps Users](docs/images/how-it-helps.png)

A business user starts with a question. They connect once, explore schemas visually, ask the AI to generate SQL, validate and transform data, track lineage to understand dependencies, and share insights — all without leaving the platform. The result: **less tool switching, fewer handoffs, faster decisions**.

---

## End-to-End Data Process with DataCopilot

![End-to-End Data Process](docs/images/end-to-end-process.png)

DataCopilot covers every stage of the data lifecycle in a single governed platform:

| Stage | What DataCopilot Does |
|---|---|
| **Connect & Ingest** | Secure connections to any database, cloud platform, file, or API |
| **Explore Data** | Auto-discover schemas, tables, columns, relationships, and metadata |
| **Transform & ETL** | Build data flows, clean, join, and enrich data with reusable components |
| **Validate Quality** | Run data quality checks, preview results, detect anomalies |
| **Lineage & Impact** | Full end-to-end lineage — know what will break before it breaks |
| **AI Insights** | Ask questions in natural language, generate SQL, explain trends |
| **Reporting & Decisions** | Deliver dashboards, summaries, and self-service analytics to the business |

**Key Benefits:** End-to-end on one platform · Faster time to insights · Self-service for business & technical users · Consistent & trusted data · Proactive impact visibility · Lower cost, higher efficiency

---

## Stack

- Backend: FastAPI, SQLAlchemy, SQLite app store, PostgreSQL/MySQL/Snowflake connectors
- Frontend: React with Vite
- Auth: JWT-based login/signup with role checks
- AI: OpenAI API for SQL and metadata assistance

## Project Layout

```text
backend/   FastAPI application
frontend/  React + Vite application
.env       Root environment file used by the backend
.env.local Optional local override file for secrets on one machine
```

## Prerequisites

- Python 3.10+
- Node.js 18+
- npm
- A PostgreSQL database for `APP_DB_URL`; local development uses a single `dlcopilot` database

## Environment Setup

1. Create a root `.env` file from `.env.example`, or set variables directly in your shell/deployment environment.
2. Set `APP_DB_URL` to your SQLAlchemy connection string.
3. Set `SECRET_KEY` to a valid Fernet key.
4. Add `OPENAI_API_KEY` if you want AI SQL features enabled.
5. Set `CORS_ALLOW_ORIGINS` to the frontend URLs allowed to call your backend (comma-separated).
6. If needed, put machine-specific overrides in `.env.local`.

The backend loads environment values from the process environment first, then `.env`, then `.env.local` for local overrides.

Example Fernet key generation:

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

## Install Dependencies

### Backend

```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

### Frontend

```bash
cd frontend
npm install
```

## Run Locally

### Start the backend

From the repository root:

```bash
venv\Scripts\python -m uvicorn app.main:app --app-dir backend --reload --host 0.0.0.0 --port 8000
```

If you prefer to run from `backend/`:

```bash
..\venv\Scripts\python -m uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

### Start the frontend

```bash
cd frontend
npm run dev
```

The frontend expects the backend at `http://localhost:8000`.

To override API base URL, set `VITE_API_BASE_URL` before running frontend commands.

Example (PowerShell):

```powershell
$env:VITE_API_BASE_URL="http://localhost:8000"
npm run dev
```

## Single Database Layout

Local development uses one PostgreSQL database: `dlcopilot`.

- `src` schema: business/demo tables and analytics views used for exploration
- `public` schema: backend auth tables such as `users`, `connections`, and `module_access`
- `app_store` schema: saved connections, recipes, prompt history, and pipeline builder state used by legacy routes
- `CORE` schema: reserved empty schema for future curated/core-layer objects

The project no longer depends on `backend/app/copilot_app.db` for runtime app data.

## Seed Demo Data

To rebuild the local `dlcopilot` PostgreSQL database with a realistic e-commerce dataset and reset the old saved connection list:

```powershell
python backend/scripts/seed_demo_environment.py
```

What this does:

- Rebuilds the `src` schema inside `dlcopilot` with a small, business-friendly e-commerce model (roughly 4.5k rows total) across customers, products, orders, order items, inventory, payments, shipments, reviews, returns, and analytics views.
- Ensures an empty `CORE` schema exists.
- Updates saved app connections to use `src` as the default schema.

If the backend is already running, restart it after seeding so startup logs and in-memory state are refreshed.

## Deploy Frontend on GitHub Pages

This repo includes a workflow at `.github/workflows/deploy-frontend-pages.yml` that builds `frontend/` and deploys to GitHub Pages on every push to `main`.

1. In GitHub, open repository Settings → Pages.
2. Under Build and deployment, set Source to GitHub Actions.
3. In Settings → Secrets and variables → Actions → Variables, add:
	- `VITE_API_BASE_URL`: your deployed backend URL (for example `https://your-backend.example.com`).
4. Push to `main` and wait for the workflow to finish.
5. Your site will be available at:
	- `https://<your-username>.github.io/DLcopilot/`

## API Key Safety (Important)

- Do not place `OPENAI_API_KEY` in frontend code, `.env` files under `frontend/`, or GitHub Pages variables used in frontend builds.
- GitHub Pages is static hosting; anything bundled into frontend JavaScript is public.
- Keep `OPENAI_API_KEY` only on the backend host (Render/Railway/Fly.io/EC2/etc.) as a server environment variable.
- Frontend should call your backend API, and backend calls OpenAI.

In production, set backend `CORS_ALLOW_ORIGINS` to include your GitHub Pages domain, for example:

```env
CORS_ALLOW_ORIGINS=https://charan1810.github.io
```

## Deploy Backend on Render

This repository now includes `render.yaml` for Render Blueprint deployment.

1. Go to Render and create a new Web Service from this GitHub repo.
2. Render will detect `render.yaml` and prefill build/start settings.
3. Set required environment variables in Render:
	- `APP_DB_URL`: production SQLAlchemy URL (example: `postgresql+psycopg2://user:pass@host:5432/dbname`)
	- `SECRET_KEY`: Fernet key used by the app
	- `JWT_SECRET`: JWT signing secret (can be same as `SECRET_KEY`, but recommended separate)
	- `OPENAI_API_KEY`: your OpenAI key (backend only)
4. Keep `CORS_ALLOW_ORIGINS` including your GitHub Pages origin:
	- `https://charan1810.github.io`
5. Deploy and copy your Render backend URL, for example:
	- `https://dlcopilot-api.onrender.com`
6. In GitHub repo Settings -> Secrets and variables -> Actions -> Variables, set:
	- `VITE_API_BASE_URL=https://dlcopilot-api.onrender.com`
7. Push to `main` (or rerun the Pages workflow) to rebuild frontend with the correct backend URL.

Notes:

- Do not put `OPENAI_API_KEY` into GitHub Pages variables.
- If your `APP_DB_URL` provider gives `postgres://...`, convert it to `postgresql+psycopg2://...`.

## GitHub Readiness

- `.gitignore` excludes local environments, dependency folders, databases, logs, and env files.
- `.gitattributes` normalizes line endings across platforms.
- `.env.example` and `.env.local.example` provide safe configuration templates without secrets.
- `CONTRIBUTING.md` and `SECURITY.md` document contribution and secret-handling expectations.

## Notes Before Pushing to GitHub

- Do not commit `.env` or `.env.local`.
- Do not commit local virtual environments or `frontend/node_modules`.
- Rotate any API key that was previously committed or shared locally.
- The repository now includes a safe `.env.example` template for onboarding.

## Main Features

- Saved database connections across PostgreSQL, MySQL, and Snowflake
- Schema explorer for databases, schemas, tables, views, and object definitions
- Sample data and relationship inspection
- Query runner and lineage views
- Pipeline builder with scheduler integration
- User authentication and admin user management
- OpenAI-assisted SQL generation and object resolution