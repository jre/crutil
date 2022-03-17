#!/bin/sh

./venv/bin/supervisorctl -c venv/supervisord.conf "$@"
