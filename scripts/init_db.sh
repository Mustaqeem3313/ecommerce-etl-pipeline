#!/bin/bash
# scripts/init_db.sh
# Runs inside the Postgres container on first startup.
# Creates the staging database (separate from Airflow's metadata DB).

set -e

STAGING_DB="${STAGING_DB:-ecommerce_staging}"
STAGING_USER="${STAGING_USER:-staging_user}"
STAGING_PASSWORD="${STAGING_PASSWORD:-staging_pass}"

echo ">>> Creating staging database: $STAGING_DB"

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    -- Create dedicated user for the staging layer
    DO \$\$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '${STAGING_USER}') THEN
            CREATE ROLE ${STAGING_USER} LOGIN PASSWORD '${STAGING_PASSWORD}';
        END IF;
    END
    \$\$;

    -- Create the staging database
    SELECT 'CREATE DATABASE ${STAGING_DB} OWNER ${STAGING_USER}'
    WHERE NOT EXISTS (
        SELECT FROM pg_database WHERE datname = '${STAGING_DB}'
    )\gexec

    GRANT ALL PRIVILEGES ON DATABASE ${STAGING_DB} TO ${STAGING_USER};
EOSQL

echo ">>> Initialising staging schema in $STAGING_DB"

psql -v ON_ERROR_STOP=1 \
     --username "$STAGING_USER" \
     --dbname "$STAGING_DB" \
     --file /docker-entrypoint-initdb.d/init_staging.sql

echo ">>> Staging database initialised successfully ✓"
