#!/bin/bash
set -e pipefail

git lfs install && git secrets --install -f

# Install Poetry
cd /app
poetry config virtualenvs.create false
poetry install --no-root