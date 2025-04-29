#!/usr/bin/env bash

set -e

export PGPASSWORD=$DB_PASS

echo "Migrating"
tools/./migrate_db.sh

echo "Seeding"
tools/./seed.sh

echo "Start main process"
python -m app.main
