#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"
command -v brew >/dev/null || { echo "Homebrew is required: https://brew.sh"; exit 1; }
command -v uv >/dev/null || brew install uv
brew list vllm-metal >/dev/null 2>&1 || brew install vllm-project/vllm-metal/vllm-metal
uv venv .venv --python 3.12
.venv/bin/uv pip install -r requirements.lock.txt
mkdir -p data logs
echo "Installed. Start everything with: ./Start.command"
echo "Notebook: Agent_Network_Lab.ipynb"
