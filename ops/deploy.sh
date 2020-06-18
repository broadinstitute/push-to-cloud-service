#!/usr/bin/env bash

UNSET="\033[0m"
BRIGHT_RED="\033[0;91m"
BOLD="\033[1m"

THIS=$(basename ${0})
JAR=$PWD/target/push-to-cloud-service.jar

bold() {
    echo -e "${BOLD}${1}${UNSET}"
}

red() {
    echo -e "${BRIGHT_RED}${1}${UNSET}"
}

error() {
    local message=${1}
    echo -e "$(bold ${THIS}:) $(red $(bold error:)) $message" 1>&2

    local details=${2}
    if [ -n "$details" ]; then
        echo -e "$(bold ${THIS}:) $details" 1>&2
    fi
}

if [ ! -e ${JAR} ]; then
    error "no such file: push-to-cloud-service.jar" \
          "run build.sh before deploying Push-To-Cloud"
    exit 1
fi

error "failed to deploy - not implemented"
exit 1
