#!/bin/sh

if [ ! -f fight-simulator-cli ]; then
    echo 'error: missing fight-simulator-cli program' >&2
    exit 1
fi

cd venv && ./bin/supervisord -c ../supervisord.conf
