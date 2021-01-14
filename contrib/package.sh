#!/usr/bin/env bash
set -e

bold() { printf '\e[1;1m%-6s\e[m\n' "$*"; }
red() { printf '\e[1;91m%-6s\e[m\n' "$*"; }
error() {
    >&2 echo -e $(bold $(basename ${0}):) $(red $(bold 'error:')) ${1}
}

validate() {
    local var="$1"
    if [ -z "${!var}" ]; then
        error "${var} must be set!"
        exit 1
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
    cp in/unit/ptc-vault.service "${out}/in/unit"

    mkdir -p "${out}/lib"
    cp target/push-to-cloud-service.jar "${out}/lib"

    if [[ -n "${NOTES}" ]]; then
        mkdir -p "${out}/docs"
        cp "${NOTES}" "${out}/docs"
    fi

    pushd derived
    tar -czvf "${release}".tar.gz "${release}"
    popd
}

main "$@"
