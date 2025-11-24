#!/bin/bash

set -e

echo "Building Docker image for Walrus tests..."
docker-compose build

echo ""
echo "Running Walrus tests in Docker..."
docker-compose run --rm walrus-test
