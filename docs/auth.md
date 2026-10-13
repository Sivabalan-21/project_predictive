# API authentication and roles (Phase 4)

## Summary
* `POST /api/v1/auth/login` exchanges `{"username","password"}` for a **JWT bearer token valid for 15 minutes**.
* Send it as `Authorization: Bearer <token>`.
* Public (no token): `GET /health`, `GET /ready`, `GET /metrics`, `POST /api/v1/auth/login`, and `/docs`, `/redoc`, `/openapi.json` when `ENABLE_DOCS` is on.
* Everything else under `/api/v1` needs a token.

| Role | May do |
|---|---|
| `viewer` | every `GET` endpoint (predictions, machines, alerts, `GET /api/v1/auth/me`) |
| `operator` | everything a viewer can **+** `POST /api/v1/alerts/{id}/resolve` |
| `admin` | everything an operator can; **reserved** for future administrative endpoints (none exist yet) |

Errors use the standard body `{"error","detail"}`: `401 unauthorized` (missing/invalid/expired token, with `WWW-Authenticate: Bearer`),
`403 forbidden` (valid token, insufficient role). Wrong username, wrong password, unknown user and disabled user all return the *same* 401.

```text
curl -s localhost:8000/api/v1/auth/login -H 'content-type: application/json' -d '{"username":"alice","password":"..."}'
curl -s localhost:8000/api/v1/alerts/active -H "Authorization: Bearer $TOKEN"
```

## Design
* **Passwords**: Argon2id (`argon2-cffi` defaults). Only the hash is stored (`users.password_hash`); plaintext is never stored or logged.
  Unknown/disabled users still cost one Argon2 verification so timing does not reveal which usernames exist.
* **Tokens**: PyJWT, **HS256 only** (the algorithm is pinned on decode, so `alg:none` and other algorithms are rejected). Claims:
  `iss, sub (user id), role, iat, exp, jti`. `exp` is mandatory and a token whose lifetime exceeds 15 minutes is rejected even if correctly signed.
* **Secret**: `JWT_SECRET`, validated at start-up: required, >= 32 characters, some variety; with `APP_ENV=production` placeholders
  (`change-me`, `example`, ...) are refused. Secret values are never put in error messages or logs.
* **Roles** are checked by FastAPI dependencies (`api/auth.py`) *before* the handler runs, so a viewer's `resolve` request never touches the database.

## Users
Table `users` (migration `0002`): `username` (lower-case, unique), `password_hash`, `role` (`viewer|operator|admin`, CHECK-constrained), `is_active`, `created_at`.

Create users **only** with the CLI. The password is never a command-line argument (it would leak through shell history and `ps`): the only
ways in are the interactive prompt and `--password-stdin`. Option abbreviations are disabled and any other option (e.g. `--password`,
`--pass`, `-p`) is rejected by the parser.

```bash
python -m auth.cli create-user --username alice --role operator            # prompts twice, no echo
printf '%s' "$PW" | python -m auth.cli create-user --username svc-ci --role viewer --password-stdin
```
```powershell
$env:PW | python -m auth.cli create-user --username svc-ci --role viewer --password-stdin
```
Policy: username 3-64 chars (`a-z 0-9 . _ -`), password 12-128 chars, not containing the username, not a common/placeholder value.
Exit codes: `0` created, `1` rejected (policy / already exists), `2` usage or configuration, `3` database error (e.g. migration not applied).
In Docker: `printf '%s' "$PW" | docker compose --profile app run --rm -T api python -m auth.cli create-user --username alice --role operator --password-stdin`.

There is no CLI/API yet to change a password, change a role or deactivate a user (set `is_active=false` / update `role` in SQL, effective when the user's token expires).

## Migration
```bash
alembic upgrade head        # applies 0002: creates ONE new table, touches no existing data
alembic downgrade 0001      # rollback: drops only the users table (accounts are lost; recreate with the CLI)
```

## Limitations
* **Tokens are stateless**: disabling a user or changing a role takes effect when the current token expires (<= 15 minutes). There is no revocation list and no logout.
* **No refresh tokens**: clients log in again every 15 minutes.
* **Login throttle**: each API process limits attempts by direct client IP (`LOGIN_RATE_LIMIT=10` in `LOGIN_RATE_WINDOW_SECONDS=60` by default) and returns 429 after the limit. It deliberately does not use the submitted username. This is process-local; a replicated deployment still needs a reverse proxy/WAF or shared rate limiter.
* HS256 uses one shared secret: anyone who knows `JWT_SECRET` can mint any token. Keep it in a secret store and rotate by changing it (all tokens become invalid).
* **TLS is not provided**; terminate HTTPS in front of the API. Tokens and passwords travel in clear text otherwise.
* `/metrics` is public (it contains no user data); restrict it at the network level.
