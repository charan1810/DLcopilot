# Security Policy

## Reporting

If you discover a security issue, do not open a public issue with exploit details.

Share the report privately with the maintainers and include:

- A short description of the issue
- Impacted area or file
- Reproduction steps
- Suggested mitigation, if known

## Secret Handling

- Do not commit API keys, database passwords, tokens, or `.env` files.
- Use environment variables, `.env`, or `.env.local` only on trusted local or deployment environments.
- Rotate any secret immediately if it is exposed in code, logs, screenshots, or commit history.

## Scope

This project handles authentication, database connection details, and AI provider credentials. Changes affecting those areas should be reviewed carefully before merge.