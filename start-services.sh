#!/bin/sh

if [ ! -f fight-simulator-cli ]; then
    echo 'error: missing fight-simulator-cli program' >&2
    exit 1
fi

script='import string, appdirs, sys; sys.stdout.write(string.Template(sys.stdin.read()).substitute({"STATEDIR": appdirs.user_data_dir("crutil")}))'

if ./ctl.sh version > /dev/null 2>&1; then
    echo "supervisord was already running"
else
    rm -f venv/supervisord.conf
    ./venv/bin/python -c "$script" \
        < supervisord.conf.in > venv/supervisord.conf
    cd venv && ./bin/supervisord -c ./supervisord.conf
    echo "Started supervisord"
fi
echo "To view service status run:"
echo "./ctl.sh status"
