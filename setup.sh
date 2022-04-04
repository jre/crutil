#!/bin/sh

set -e
topdir="$(pwd)"
uname_sp="$(uname -sp)"
set -x

case "$uname_sp" in
    Darwin\ i386)
        export ARCHFLAGS='-arch x86_64'
        ;;
    OpenBSD\ *)
        test -d venv || python3 -m venv --system-site-packages venv
        ;;
esac

test -d venv || python3 -m venv venv
export TMPDIR="${topdir}/venv/tmp"
mkdir -p "$TMPDIR"
./venv/bin/pip install appdirs flask flup requests supervisor web3
