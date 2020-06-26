#!/usr/bin/env bash

THIS=$(basename ${0})
JAR=$PWD/target/push-to-cloud-service.jar

bold() { printf '\e[1;1m%-6s\e[m\n' "$*"; }
red() { printf '\e[1;91m%-6s\e[m\n' "$*"; }

error() {
    local message=${1}
    echo -e $(bold ${THIS}:) $(red $(bold 'error:')) $message 1>&2

    local details=${2}
    if [ -n "${details}" ]; then
        echo -e $(bold ${THIS}:) $details 1>&2
    fi
}

if [ ! -e ${JAR} ]; then
    error 'no such file: push-to-cloud-service.jar' \
          'run build.sh before deploying Push-To-Cloud'
    exit 1
fi

error 'failed to deploy - not implemented'
exit 1
