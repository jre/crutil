#!/bin/sh

set -e

python3 -m venv venv
./venv/bin/pip install appdirs requests google-api-python-client google-auth-httplib2 google-auth-oauthlib
