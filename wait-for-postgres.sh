#!/bin/bash

set -e

host="$1"
shift
cmd="$@"

echo "Waiting for Postgres at $host:5432..."

until PGPASSWORD=airflow psql -h "$host" -U airflow -d airflow -c '\q' 2>/dev/null; do
  >&2 echo "Postgres is unavailable - sleeping"
  sleep 2
done

>&2 echo "Postgres is up - executing command"
exec $cmd
