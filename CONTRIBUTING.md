# Contributing

## Setup

1. Install Python and Node.js prerequisites described in the repository README.
2. Create local environment variables using `.env.example` and, if needed, `.env.local`.
3. Install backend dependencies with `pip install -r requirements.txt`.
4. Install frontend dependencies with `npm install` inside `frontend/`.

## Development Workflow

1. Keep secrets out of tracked files. Use environment variables or ignored local env files.
2. Make focused changes and avoid unrelated formatting churn.
3. Update documentation when setup, behavior, or API expectations change.
4. Verify the backend starts and the frontend builds before opening a pull request.

## Pull Requests

1. Use a short, specific title.
2. Describe the user-visible change and any required setup changes.
3. Mention test coverage or manual verification performed.
4. Call out security-sensitive changes explicitly, especially around auth, secrets, and database access.