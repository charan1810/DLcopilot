# DataCopilot

DataCopilot is a full-stack database exploration and AI-assisted engineering tool built with FastAPI and React. It supports saved database connections, schema browsing, object metadata inspection, lineage views, query execution, pipeline scheduling, role-based auth, and OpenAI-backed SQL assistance.

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
- A database for `APP_DB_URL` used by SQLAlchemy

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