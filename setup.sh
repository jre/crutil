#!/bin/sh

set -e

python3 -m venv venv
./venv/bin/pip install appdirs requests web3
