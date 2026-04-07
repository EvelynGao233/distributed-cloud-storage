#!/usr/bin/env bash
# Build and run a local container with gRPC/Protobuf (no course Docker Hub access required).
set -euo pipefail
IMAGE_NAME="${CLOUD_STORAGE_DEV_IMAGE:-cloud-storage-dev}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if [[ "${1:-}" == "--build" ]] || ! docker image inspect "$IMAGE_NAME" &>/dev/null; then
  echo "[cloud-storage] Building image $IMAGE_NAME (gRPC from source; first time is slow) ..."
  docker build -t "$IMAGE_NAME" -f Dockerfile .
fi

echo "[cloud-storage] Starting container (mount: $SCRIPT_DIR -> /workspace) ..."
exec docker run -it --rm --name cloud-storage-dev \
  -w /workspace \
  -v "$SCRIPT_DIR":/workspace \
  -p 2500:2500 \
  -p 11000:11000 \
  -p 5050-5100:5050-5100 \
  -p 8080-8100:8080-8100 \
  "$IMAGE_NAME"
