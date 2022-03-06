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
        pkg_info -q \
                 -e 'wxWidgets-gtk3-*' \
                 -e 'wxWidgets-media-*' \
                 -e 'wxWidgets-webkit-*'
        export MAKE=gmake
        test -d venv || virtualenv --system-site-packages venv
        ;;
esac

test -d venv || python3 -m venv venv
export TMPDIR="${topdir}/venv/tmp"
mkdir -p "$TMPDIR"
./venv/bin/pip install appdirs requests web3

case "$uname_sp" in
    OpenBSD\ *)
        ./venv/bin/pip show -qq wxPython && exit
        bdist="$(echo wxPython-4.0.*-openbsd*.tar.gz | \
            sed 's/\.tar\.gz$//' | tail -1)"

        if [ ! -f "${bdist}.tar.gz" ]; then
            cd venv
            ./bin/pip download --no-deps 'wxpython<4.1'
            wxpy="$(echo wxPython-4.0.*.tar.gz | \
                sed 's/\.tar\.gz$//' | tail -1)"
            test -f "${wxpy}.tar.gz"
            mkdir -p build
            test -d "build/${wxpy}" || tar -C build -xzf "./${wxpy}.tar.gz"
            cd "build/$wxpy"
            patch < "${topdir}/${wxpy}.patch"
            "${topdir}/venv/bin/pip" install -r requirements.txt
            env CPPFLAGS="-I${topdir}/build/${wxpy}/ext/wxWidgets/include" \
                "${topdir}/venv/bin/python" build.py build bdist \
                -v -j "$(sysctl -n hw.ncpu)" \
                --extra_waf="-k -p" --use_syswx
            cp "dist/${wxpy}"-*.tar.gz "${topdir}/"
            cd "$topdir"
        fi

        bdist="$(echo wxPython-4.0.*-openbsd*.tar.gz | \
            sed 's/\.tar\.gz$//' | tail -1)"
        test -f "${bdist}.tar.gz"
        tar -C venv -xzf "./${bdist}.tar.gz"
        mv "venv/${bdist}"/wx* venv/lib/python*/site-packages/
        rm -rf "venv/${bdist}"
        ;;
    *)
        ./venv/bin/pip install wxpython
        ;;
esac
