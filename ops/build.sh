#!/bin/bash -e

# This script is used to compile the source code with AOT and
# built it into an executable UberJar. For more information,
# please check here:
# - https://clojure.org/guides/deps_and_cli#aot_compilation
# - https://github.com/tonsky/uberdeps

# 1. Aot compile
clj -e "(compile 'ptc.start)"

# 2. Uberjar with --main-class option
clojure -A:uberdeps --main-class ptc.start
