#!/usr/bin/env bash

set -e

GREEN="\033[32m"
YELLOW="\033[33m"
BLUE="\033[34m"
NC="\033[0m"

echo -e "${GREEN}👀 Checking migrations ${NC}"

source <(grep dsn app.conf | sed -r 's/\+psycopg//' | sed 's/=/="/; s/$/"/')

# check if the migration table exists
if
  psql $dsn -t -c "\dt" | grep 'migrations' >/dev/null
  [ $? -eq 1 ]
then
  echo -e "${YELLOW}⚠️ Migration table does not exists. Creating migrations table.${NC}"

  # create the migration table
  echo "CREATE TABLE migrations (id serial PRIMARY KEY, name VARCHAR(255) NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);" | psql $dsn -q -o /dev/null
fi

for file in sql/*.sql; do
  # Check each SQL file to see if it's already in the migrations table
  if psql $dsn -c "SELECT name FROM migrations WHERE name = '$file';" | grep -q $file; then
    echo -e "${YELLOW}⏩ File $file is already in the migrations table. Skipping.${NC}"
  else
    echo -e "${GREEN}▶️ Running migration $file${NC}"
    psql $dsn -f $file -q -o /dev/null
    echo "INSERT INTO migrations (name) VALUES ('$file');" | psql $dsn -o /dev/null
  fi
done
