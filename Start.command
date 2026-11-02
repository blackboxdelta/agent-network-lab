#!/bin/zsh
set -e
cd "$(dirname "$0")"
.venv/bin/python manage.py start
open http://127.0.0.1:8020
