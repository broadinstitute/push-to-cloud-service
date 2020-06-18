#!/usr/bin/env bash

# Build Uberjar with --main-class option. See for more
# information, see https://github.com/tonsky/uberdeps

if [ ${USE_CPCACHE-1} -eq 0 ]; then
    CLJ_FLAGS=-Sforce
fi

clojure $CLJ_FLAGS -A:uberdeps --main-class ptc.start
