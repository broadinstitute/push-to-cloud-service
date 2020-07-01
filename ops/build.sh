#!/usr/bin/env bash

# Build push-to-cloud service jar without the classpath cache,
# running build-time (unit) tests.

set -e

info() {
    local BOLD='\e[1;1m%-6s\e[m\n'
    printf $BOLD "$*"
}

logged() { echo "$*"; "$@"; }

main() {
    info '=> building push-to-cloud-service.jar'
    logged clojure -Sforce -A:build

    info '=> running unit tests'
    logged clojure -Sforce -A:test unit
}

main
