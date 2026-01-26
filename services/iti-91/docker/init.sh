#!/usr/bin/env bash

set -e

export PGPASSWORD=$DB_PASS

echo "Migrating"
tools/./migrate_db.sh

echo "Seeding"
tools/./seed.sh

echo "----------------------------------------------"
echo " Do not run this on a production environment! "
echo "----------------------------------------------"
echo ""
echo "This image is not suitable for: "
echo " - a production environment;"
echo " - an environment that contains sensitive information;"
echo " - an environment that should remain secure."
echo ""

_DO_NOT_USE_ON_PRODUCTION="I'm a fool if I run this application on a production environment."

if [ "$DO_NOT_USE_ON_PRODUCTION" != "$_DO_NOT_USE_ON_PRODUCTION" ]; then
  echo "If you want to continue, set the \"DO_NOT_USE_ON_PRODUCTION\" variable"
  echo " to \"$_DO_NOT_USE_ON_PRODUCTION\""
  echo ""
  exit 1
fi

echo "Start main process"
python -m app.main
