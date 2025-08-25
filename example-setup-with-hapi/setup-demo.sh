#!/bin/bash

# Setup script for mTLS demo
# This script generates certificates and starts the demo environment

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "Setting up mTLS demo environment..."

chmod +x generate-certs.sh
chmod +x generate-keystores.sh

if [ ! -f certificates/ca.crt ]; then
    echo "Generating TLS certificates..."
    ./generate-certs.sh
    ./generate-keystores.sh
else
    echo "TLS certificates already exist, skipping generation..."
fi

echo ""
echo "Starting Docker Compose environment with mTLS..."
docker-compose up -d

echo ""
echo "mTLS demo setup complete!"
echo ""
