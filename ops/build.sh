#!/usr/bin/env bash
# Build Uberjar with --main-class option. See for more
# information, see https://github.com/tonsky/uberdeps

set -e

THIS=$(basename ${0})

usage() {
    cat <<EOF
Usage:
    $THIS [FLAGS]...

Build push-to-cloud service jar, running build-time tests; see
https://github.com/broadinstitute/push-to-cloud-service for more
information.

Flags:
    --clean              clean output directories before build
    --help               show this message
    --no-classpath-cache recompute the classpath, don't cache
    --no-tests           don't run build-time tests
    --silent             don't output to stdout
    --verbose            be verbose

$THIS runs 'clojure \${CLOJURE_OPTS[@]}", where by default
CLOJURE_OPTS is empty. You can override this to pass additional
options and flags to clojure.
EOF
}

CLEAN=0
TEST=1
SILENT=0
VERBOSE=0

while [ $# -gt 0 ];
do
    case "$1" in
        --clean)
            CLEAN=1
            ;;
        --help)
            usage
            exit 0
            ;;
        --no-classpath-cache)
            CLOJURE_OPTS+=('-Sforce')
            ;;
        --no-tests)
            TEST=0
            ;;
        --silent)
            SILENT=1
            ;;
        --verbose)
            VERBOSE=1
            CLOJURE_OPTS+=('-Sverbose')
            ;;
        *)
            echo "$THIS: unrecognized argument '$1'" >&2
            echo "Try '$THIS --help' for more information." >&2
            exit 1
            ;;
    esac
    shift
done

when() {
    if [ "$1" -ne 0 ]; then
        shift
        "$@"
    fi
}

info() {
    local BOLD='\e[1;1m%-6s\e[m\n'
    printf $BOLD "$*"
}

doIO() {
    when $VERBOSE echo "$*"
    "$@"
}

clean-outputs() {
    info '=> cleaning output directories'
    doIO rm -rf target/ derived/ .cpcache
}

make-ptc-jar() {
    info '=> building push-to-cloud-service.jar'
    doIO mkdir -p 'derived'
    doIO clojure "${CLOJURE_OPTS[@]}" -A:uberdeps -main-classpath ptc.start \
        | tee 'derived/build.log' 2>&1
}

run-tests() {
    info '=> running unit tests'
    doIO mkdir -p 'derived/test'
    doIO clojure "${CLOJURE_OPTS[@]}" -A:test unit \
        | tee 'derived/test/unit.log' 2>&1
}

main() {
    when $CLEAN clean-outputs
    make-ptc-jar
    when $TEST run-tests
}

when $SILENT eval 'exec 1>/dev/null'
main
