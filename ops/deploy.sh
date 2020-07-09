#!/usr/bin/env bash

THIS=$(basename ${0})
JAR="${1}"

bold() { printf '\e[1;1m%-6s\e[m\n' "$*"; }
red() { printf '\e[1;91m%-6s\e[m\n' "$*"; }

usage() {
    cat <<EOF
Usage:
   ${THIS} JAR_FILE
EOF
}

error() {
    >&2 echo -e $(bold ${THIS}:) $(red $(bold 'error:')) ${1}
}

if [ -z ${JAR} ]; then
    error 'missing jar file'
    usage
    exit 1
fi

if [ ! -e ${JAR} ]; then
    error "no such file: ${JAR}"
    exit 1
fi

error 'failed to deploy - not implemented'
exit 1
