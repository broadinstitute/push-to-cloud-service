#!/usr/bin/env bash

# Build Uberjar with --main-class option. See for more
# information, see https://github.com/tonsky/uberdeps

clojure -Sforce -A:uberdeps --main-class ptc.start
