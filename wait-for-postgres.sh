#!/bin/sh
# wait-for-postgres.sh

until pg_isready -h "$DB_HOST" -p "$DB_PORT"; do
  echo "Waiting for PostgreSQL at $DB_HOST:$DB_PORT..."
  sleep 1
done

echo "PostgreSQL is up – executing command"
exec "$@"