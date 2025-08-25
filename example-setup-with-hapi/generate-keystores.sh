#!/bin/bash

# Generate Java keystores from OpenSSL certificates
# This script converts the certificates to Java keystores for HAPI FHIR

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CERT_DIR="$SCRIPT_DIR/certificates"

cd "$CERT_DIR"

KEYSTORE_PASSWORD="secret"

echo "Generating Java keystores from certificates in $CERT_DIR"

if [ ! -f ca.crt ] || [ ! -f server.crt ] || [ ! -f server.key ] || [ ! -f directory-server.crt ] || [ ! -f directory-server.key ] || [ ! -f client.crt ] || [ ! -f client.key ]; then
    echo "Error: Required certificate files not found. Run generate-certs.sh first."
    exit 1
fi

rm -f *.p12

# 1. Create server keystore (PKCS12) with server certificate and private key
echo "1. Creating update-client server keystore..."
openssl pkcs12 -export -in server.crt -inkey server.key -out server-keystore.p12 -name "hapi-server" -passout pass:$KEYSTORE_PASSWORD

# 1a. Create directory server keystore (PKCS12) with directory server certificate and private key
echo "1a. Creating directory server keystore..."
openssl pkcs12 -export -in directory-server.crt -inkey directory-server.key -out directory-server-keystore.p12 -name "hapi-directory-server" -passout pass:$KEYSTORE_PASSWORD

# 2. Create truststore with CA certificate
echo "2. Creating truststore with CA certificate..."
keytool -import -trustcacerts -noprompt -alias ca -file ca.crt -keystore truststore.p12 -storetype PKCS12 -storepass $KEYSTORE_PASSWORD

# 3. Create client keystore (for client applications if needed)
echo "3. Creating client keystore..."
openssl pkcs12 -export -in client.crt -inkey client.key -out client-keystore.p12 -name "mcsd-client" -passout pass:$KEYSTORE_PASSWORD

chmod 644 *.p12 *.crt *.key

echo "Java keystore generation complete!"
echo "All keystores use password: $KEYSTORE_PASSWORD"
