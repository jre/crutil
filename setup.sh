#!/bin/sh

set -e

case "$(uname -sp)" in
    Darwin\ i386)
        export ARCHFLAGS='-arch x86_64'
        ;;
esac

python3 -m venv venv
./venv/bin/pip install appdirs requests web3
