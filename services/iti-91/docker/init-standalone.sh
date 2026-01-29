#!/usr/bin/env bash

set -e

if [ ! -e /src/app.conf ]; then
  echo "----------------------------------------------"
  echo " APP.CONF IS NOT MOUNTED"
  echo "----------------------------------------------"
  echo ""
  echo "In order to run this module standalone, an app.conf"
  echo "is needed. Please mount an existing iti-91.conf into"
  echo "the container as /src/app.conf in order to run."
  echo ""
  echo "    docker run --mount type=bind,source=./iti-91.conf,target=/src/app.conf ..."
  echo ""
  exit 1
fi

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

echo "--------------------------------------------"
echo "Starting main application"
echo "--------------------------------------------"

python -m app.main
