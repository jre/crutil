#!/bin/sh

./venv/bin/supervisorctl -s "unix://`pwd`/venv/supervisor.sock" "$@"
