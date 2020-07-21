#!/usr/bin/env bash
set -ex

THIS=$(basename ${0})

bold() { printf '\e[1;1m%-6s\e[m\n' "$*"; }
red() { printf '\e[1;91m%-6s\e[m\n' "$*"; }
error() { >&2 echo -e $(bold ${THIS}:) $(red $(bold 'error:')) ${1}; }

validate() {
    local var="$1"
    if [ -z "${!var}" ]; then
        error "${var} must be set!"
    fi
}

main() {
    validate VERSION
    local release="push-to-cloud-service-${VERSION}"
    local out="derived/${release}"

    mkdir -p "${out}"

    echo "${VERSION}" > "${out}/version"

    cp in/install.sh.in "${out}/install.sh"
    chmod +x "${out}/install.sh"

    cp LICENSE "${out}"

    mkdir -p "${out}/in/unit"
    cp in/unit/push-to-cloud.service.in "${out}/in/unit"

    mkdir -p "${out}/lib"
    cp target/push-to-cloud-service.jar "${out}/lib"

    if [[ -n "${NOTES}" ]]; then
        mkdir -p "${out}/doc"
        cp "${NOTES}" "${out}/doc"
    fi

    pushd derived
    tar -czvf "${release}".tar.gz "${release}"
    popd
}

main "$@"
