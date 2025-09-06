#!/bin/bash

# Generate certificates for mTLS demo
# This script creates a CA, server certificates, and client certificates

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CERT_DIR="$SCRIPT_DIR/certificates"

mkdir -p "$CERT_DIR"
cd "$CERT_DIR"

echo "Generating certificates in $CERT_DIR"

rm -f *.pem *.key *.crt *.csr *.srl *.ext

# 1. Generate CA private key
echo "1. Generating CA private key..."
openssl genrsa -out ca.key 4096

# 2. Generate CA certificate
echo "2. Generating CA certificate..."
openssl req -new -x509 -days 365 -key ca.key -out ca.crt -subj "/C=NL/ST=Netherlands/L=Demo/O=MCSD-Demo/OU=Demo-CA/CN=Demo-CA"

# 3. Generate update-client server private key
echo "3. Generating update-client server private key..."
openssl genrsa -out server.key 4096

# 4. Generate update-client server certificate signing request with SAN
echo "4. Generating update-client server CSR with SAN..."
cat > server.ext << EOF
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage = digitalSignature, nonRepudiation, keyEncipherment, dataEncipherment
subjectAltName = @alt_names

[alt_names]
DNS.1 = hapi-update-client
DNS.2 = localhost
IP.1 = 127.0.0.1
EOF

openssl req -new -key server.key -out server.csr -subj "/C=NL/ST=Netherlands/L=Demo/O=MCSD-Demo/OU=Demo-Server/CN=hapi-update-client"

# 4c. Generate server certificate signed by CA with SAN
echo "4c. Generating server certificate with SAN..."
openssl x509 -req -days 365 -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out server.crt -extfile server.ext

# 5a. Generate directory server private key
echo "5a. Generating directory server private key..."
openssl genrsa -out directory-server.key 4096

# 5b. Generate directory server certificate signing request with SAN
echo "5b. Generating directory server CSR with SAN..."
cat > directory-server.ext << EOF
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage = digitalSignature, nonRepudiation, keyEncipherment, dataEncipherment
subjectAltName = @alt_names

[alt_names]
DNS.1 = hapi-directory
DNS.2 = localhost
IP.1 = 127.0.0.1
EOF

openssl req -new -key directory-server.key -out directory-server.csr -subj "/C=NL/ST=Netherlands/L=Demo/O=MCSD-Demo/OU=Demo-Server/CN=hapi-directory"

# 5c. Generate directory server certificate signed by CA with SAN
echo "5c. Generating directory server certificate with SAN..."
openssl x509 -req -days 365 -in directory-server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out directory-server.crt -extfile directory-server.ext

# 6. Generate client private key
echo "6. Generating client private key..."
openssl genrsa -out client.key 4096

# 7. Generate client certificate signing request with SAN
echo "7. Generating client CSR with SAN..."
cat > client.ext << EOF
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage = digitalSignature, nonRepudiation, keyEncipherment, dataEncipherment
subjectAltName = @alt_names

[alt_names]
DNS.1 = mcsd-update-client
DNS.2 = localhost
IP.1 = 127.0.0.1
EOF

openssl req -new -key client.key -out client.csr -subj "/C=NL/ST=Netherlands/L=Demo/O=MCSD-Demo/OU=Demo-Client/CN=mcsd-update-client"

# 8. Generate client certificate signed by CA with SAN
echo "8. Generating client certificate with SAN..."
openssl x509 -req -days 365 -in client.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out client.crt -extfile client.ext

rm -f *.csr *.ext

chmod 644 *.crt *.key

echo "Certificate generation complete!"
