#!/bin/bash

set -euo pipefail

usage() {
    cat <<'EOF'
Usage:
  baseline.sh check <tool> <baseline-file> [tool args...]
  baseline.sh update <tool> <baseline-file> [tool args...]

Supported tools:
  golangci-lint
  gosec
EOF
}

if [ "$#" -lt 3 ]; then
    usage >&2
    exit 2
fi

command_name=$1
tool_name=$2
baseline_file=$3
shift 3

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/../.." && pwd)"
tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/go-cache-quality.XXXXXX")"

cleanup() {
    rm -rf "${tmp_dir}"
}
trap cleanup EXIT

run_tool() {
    case "${tool_name}" in
        golangci-lint)
            "${GOLANGCI_LINT_BIN:-golangci-lint}" run "$@"
            ;;
        gosec)
            # shellcheck disable=SC2086
            "${GOSEC_BIN:-gosec}" ${GOSEC_ARGS:-} -quiet -fmt=golint "$@"
            ;;
        *)
            echo "unsupported tool: ${tool_name}" >&2
            exit 2
            ;;
    esac
}

normalize_output() {
    sed "s#${repo_root}/##g" \
        | sed 's#^\./##' \
        | sed 's/^[[:space:]]*//' \
        | awk 'NF' \
        | sort -u
}

collect_findings() {
    local raw_output=$1
    local normalized_output=$2
    local status

    shift 2

    set +e
    run_tool "$@" >"${raw_output}" 2>&1
    status=$?
    set -e

    normalize_output <"${raw_output}" >"${normalized_output}"

    if [ "${status}" -ne 0 ] && [ ! -s "${normalized_output}" ]; then
        cat "${raw_output}" >&2
        exit "${status}"
    fi
}

baseline_normalized="${tmp_dir}/baseline.txt"
current_raw="${tmp_dir}/current.raw"
current_normalized="${tmp_dir}/current.txt"
new_findings="${tmp_dir}/new.txt"
resolved_findings="${tmp_dir}/resolved.txt"

case "${command_name}" in
    check)
        if [ ! -f "${baseline_file}" ]; then
            echo "baseline file not found: ${baseline_file}" >&2
            exit 1
        fi

        collect_findings "${current_raw}" "${current_normalized}" "$@"
        awk 'NF && $1 !~ /^#/' "${baseline_file}" | sort -u >"${baseline_normalized}"
        comm -23 "${current_normalized}" "${baseline_normalized}" >"${new_findings}"
        comm -13 "${current_normalized}" "${baseline_normalized}" >"${resolved_findings}"

        if [ -s "${new_findings}" ]; then
            echo "New ${tool_name} findings not present in baseline ${baseline_file}:"
            cat "${new_findings}"
            exit 1
        fi

        if [ -s "${resolved_findings}" ]; then
            resolved_count="$(wc -l <"${resolved_findings}" | tr -d ' ')"
            echo "Baseline contains ${resolved_count} ${tool_name} findings not observed in this run."
            echo "Refresh the baseline after intentional full-scope cleanup."
        fi

        if [ -s "${current_normalized}" ]; then
            count="$(wc -l <"${current_normalized}" | tr -d ' ')"
            echo "No new ${tool_name} findings beyond baseline (${count} existing findings)."
        else
            echo "No ${tool_name} findings."
        fi
        ;;
    update)
        collect_findings "${current_raw}" "${current_normalized}" "$@"
        mkdir -p "$(dirname "${baseline_file}")"
        cp "${current_normalized}" "${baseline_file}"
        count="$(wc -l <"${baseline_file}" | tr -d ' ')"
        echo "Wrote ${tool_name} baseline with ${count} findings to ${baseline_file}."
        ;;
    *)
        usage >&2
        exit 2
        ;;
esac
