#!/usr/bin/env bash

set -e

GREEN="\033[32m"
YELLOW="\033[33m"
BLUE="\033[34m"
NC="\033[0m"

source <(grep dsn app.conf | tr -d '\r' | sed -r 's/\+psycopg//' | sed 's/=/="/; s/$/"/')

echo -e "${GREEN}👀 Checking seed migrations ${NC}"

# check if the seed_migrations table exists
if
  psql $dsn -t -c "\dt" | grep 'seed_migrations' >/dev/null
  [ $? -eq 1 ]
then
  echo -e "${YELLOW}⚠️ Seed migration table does not exists. Creating seed migrations table.${NC}"

  # create the seed_migrations table
  echo "CREATE TABLE seed_migrations (id serial PRIMARY KEY, name VARCHAR(255) NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);" | psql $dsn -q -o /dev/null
fi

if ! [ -f seed ]; then
  echo -e "${GREEN}⚠️ No seed file exists"
else
  for file_path in seeds/*.py; do
    file_name=$(basename $file_path)
    # Check each SQL file to see if it's already in the migrations table
    if psql $dsn -c "SELECT name FROM seed_migrations WHERE name = '$file_name';" | grep -q $file_name; then
      echo -e "${YELLOW}⏩ File $file_path is already in the seed_migrations table. Skipping.${NC}"
    else
      echo -e "${GREEN}▶️ Running seed_migrations $file_name${NC}"
      python $file_path
      echo "INSERT INTO seed_migrations (name) VALUES ('$file_name');" | psql $dsn -o /dev/null
    fi
  done
fi
