#!/usr/bin/env bash

KEYFILE=secrets/ssl/server.key
CERTFILE=secrets/ssl/server.cert

# Do nothing if file(s) exist
if [[ -f $KEYFILE || -f $CERTFILE ]] ; then
    echo "Certification already exists in secrets/ssl directory. Not attempting to create a new certificate."
    exit 1
fi

# Create dir structure
mkdir -p `dirname $CERTFILE`

# Generate key and cert
openssl req -x509 -newkey rsa:2048 \
    -keyout $KEYFILE -out $CERTFILE -sha256 -days 3650 \
    -nodes -subj "/C=NL/L=Den Haag/O=MinVWS/OU=RDO/CN=zmodules.skeleton-service.pgo"

echo "Created certificate at $CERTFILE"
