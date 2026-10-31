#!/usr/bin/env bash
set -euo pipefail

: "${1:?Patroni connection string is required}"
: "${APP_DATABASE:?APP_DATABASE is required}"
: "${APP_USERNAME:?APP_USERNAME is required}"
: "${APP_PASSWORD:?APP_PASSWORD is required}"

psql --no-psqlrc --no-password --dbname="$1" --set=ON_ERROR_STOP=1 <<'EOSQL'
\getenv app_database APP_DATABASE
\getenv app_username APP_USERNAME
\getenv app_password APP_PASSWORD

SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', :'app_username', :'app_password')
WHERE NOT EXISTS (
  SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = :'app_username'
)\gexec

SELECT format('CREATE DATABASE %I OWNER %I ENCODING ''UTF8''', :'app_database', :'app_username')
WHERE NOT EXISTS (
  SELECT 1 FROM pg_catalog.pg_database WHERE datname = :'app_database'
)\gexec
EOSQL
