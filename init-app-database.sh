#!/bin/sh
set -eu

psql --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
    --set=app_database="$DB_NAME" \
    --set=app_user="$DB_USER" \
    --set=app_password="$DB_PW" <<'EOSQL'
CREATE ROLE :"app_user" LOGIN PASSWORD :'app_password';
CREATE DATABASE :"app_database" OWNER :"app_user";
EOSQL
