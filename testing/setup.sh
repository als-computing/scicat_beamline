#!/bin/bash
# Setup script for Prefect local server environment
# Run this after cloning the repo on a new machine

PREFECT_API_URL="http://localhost:4200/api"

CLEAN_SETUP=0
while [[ "$#" -gt 0 ]]; do
    case "$1" in
        --clean)
            CLEAN_SETUP=1
            shift
            ;;
        *)
            echo "Unknown argument: $1"
            exit 1
            ;;
    esac
done

# This script is meant to be run from the repo root.
if [ ! -d "testing" ]; then
  echo "This script is meant to be run from the repo root, e.g. ./scripts/setup.sh"
  exit 1
fi

# Do we have access to Python?
if command -v python3 &> /dev/null; then
    PYTHON=python3
elif command -v python &> /dev/null; then
    PYTHON=python
else
    echo "⚠️  Python not found. Cannot continue."
    exit 1
fi

# Check Python version is 3.12 or greater
if ! $PYTHON -c 'import sys
if sys.version_info >= (3,12):
    pass
else:
    raise SystemExit(1)
' >/dev/null 2>&1; then
    echo "⚠️  Python 3.12 or greater is required. Found:"
    $PYTHON -V
    exit 1
fi

set -e

echo "🔧 Setting up Prefect local server environment..."
echo ""

# Load environment variables
if [ -f .env ]; then
    echo "✅ Found .env file"
    set -a  # Automatically export all variables
    source .env
    set +a
else
    echo "❌ .env file not found!"
    exit 1
fi

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker first."
    exit 1
fi
echo "✅ Docker is running"

echo ""
echo "📋 Prefect Configuration:"
echo "   PREFECT_API_URL: ${PREFECT_API_URL}"
echo ""

if [ $CLEAN_SETUP -eq 1 ]; then
    echo "❎ Clean: Taking down any current Docker deployment..."
    docker compose -f testing/docker/compose.yml down

    echo "❎ Clean: Deleting any existing worker image..."
    if docker rmi prefect-ingest-worker:latest &> /dev/null; then
        echo "   Image removed"
    fi
fi

# Build custom worker image with GPU support (Doing this in advance avoids a fetch warning)
echo "🔨 Building custom worker image for SciCat ingest testing..."
docker compose -f testing/docker/compose.yml build ingest_worker

# Start Docker Compose services
echo "🚀 Starting Prefect server and workers..."
docker compose -f testing/docker/compose.yml up -d

# Wait for server to be ready
echo "⏳ Waiting for Prefect server to start..."
for i in {1..30}; do
    if curl -f ${PREFECT_API_URL}/health &> /dev/null; then
        echo "✅ Prefect server is ready!"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "❌ Prefect server failed to start in 30 seconds"
        echo "Check logs with: docker compose logs prefect_server"
        exit 1
    fi
    sleep 1
done

# Create work pool if it doesn't exist
echo ""
echo "🏊 Creating work pool..."
$PYTHON testing/prefect_toolkit.py --create_work_pool

echo ""
echo "✅ Setup complete!"
echo ""
echo "📋 Next steps:"
echo "   1. Check status: docker compose -f testing/docker/compose.yml ps"
echo "   2. View logs: docker compose -f testing/docker/compose.yml logs -f"
echo "   3. View UI: http://localhost:4200"
echo "   4. Create deployment: ${PYTHON} testing/create_deployment.py"
echo "   5. Run deployment: ${PYTHON} testing/run_deployment.py"
echo ""
echo "🛑 To stop: docker compose -f testing/docker/compose.yml down"
