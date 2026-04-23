#!/bin/bash

set -o noclobber  # Prevent overwriting existing files with >
set -o nounset    # Treat unset variables as an error
set -o errexit    # Exit immediately if a command exits with non-zero status
set -o pipefail   # Prevent errors in a pipeline from being masked
set -o errtrace   # Ensure that any error traps are inherited by functions

# shellcheck disable=SC2317 # shellcheck doesn't understand this is called
# by the ERR trap
on_fatal_error() {
    local -r exit_code="${?}"
    { set +x; } 2>/dev/null
    local -r line_number="${1:-}"
    local -r file="${BASH_SOURCE[0]}"
    echo "FATAL: Failed at ${file}:${line_number}: ${BASH_COMMAND} (exit code: ${exit_code})" >&2

    local frame=0
    local line_no func_name file_name
    echo "ERROR: Stack trace:" >&2
    while read -r line_no func_name file_name < <(caller "${frame}"); do
        echo "ERROR: ${frame}:  at ${func_name}() ${file_name}:${line_no}" >&2
        : $((frame++))
    done

    if declare -F on_fatal_cleanup >/dev/null; then
        echo "ERROR: Running on_fatal_cleanup..." >&2
        on_fatal_cleanup || true
    fi

    [[ "${exit_code}" -eq 0 ]] && exit 1
    exit $(( exit_code ))
}
trap 'on_fatal_error ${LINENO}' ERR

: "${ETHTOOL:=/usr/sbin/ethtool}"
: "${GREP:=/usr/bin/grep}"
: "${AWK:=/usr/bin/awk}"
: "${NPROC:=/usr/bin/nproc}"

declare -g config_dir="/etc/nic-tune"
declare -g interface=""

# Color codes for TTY output
declare -rg term_red='\033[38;5;203m'
declare -rg term_orange='\033[38;5;214m'
declare -rg term_bold_white='\033[1;38;5;255m'
declare -rg term_white='\033[38;5;255m'
declare -rg term_no_color='\033[0m'

panic() {
    if [[ -t 2 ]]; then
        echo -e "${term_red}FATAL:${term_no_color} $*" >&2
    else
        echo "FATAL: $*" >&2
    fi
    exit 1
}

error() {
    if [[ -t 2 ]]; then
        echo -e "${term_red}ERROR:${term_no_color} $*" >&2
    else
        echo "ERROR: $*" >&2
    fi
}

warn() {
    if [[ -t 2 ]]; then
        echo -e "${term_orange}WARN:${term_no_color} $*" >&2
    else
        echo "WARN: $*" >&2
    fi
}

info() {
    if [[ -t 1 ]]; then
        echo -e "${term_bold_white}INFO:${term_no_color} ${term_white}$*${term_no_color}"
    else
        echo "INFO: $*"
    fi
}

check_root() {
    if [[ "${EUID}" -ne 0 ]]; then
        panic "Must run as root to tune NIC!"
    fi
}

usage() {
    cat << EOF
Usage: $(basename "$0") [OPTIONS]

Tune NIC ring buffers, RSS queues, and hardware features for optimal network
performance. Designed to run at boot via systemd to persist settings.

Options:
    -h, --help         Show this help message and exit
    -c, --config-dir   Path to the config directory
                       (default: ${config_dir})
    -i, --interface    Interface name (required)

What This Script Tunes:

    Ring Buffers (rx, tx)
        Memory between NIC hardware and kernel. Larger buffers absorb traffic
        bursts without dropping packets. Typical maximums: 4096-8192 entries.

    RSS Combined Queues (combined)
        Receive Side Scaling distributes packet processing across CPU cores.
        Each queue handles both RX and TX. Set to CPU count for parallelism.

    Hardware Features (ethtool -K)
        Modern NICs offload work from CPU to hardware. Common features:

        tso  (TCP Segmentation Offload)  - NIC splits large TCP into packets
        gso  (Generic Segmentation)      - Software fallback for TSO
        gro  (Generic Receive Offload)   - Coalesces small packets into large
        lro  (Large Receive Offload)     - Hardware GRO (avoid if routing)
        rx-checksumming                  - NIC verifies packet checksums
        tx-checksumming                  - NIC computes packet checksums
        scatter-gather                   - DMA from non-contiguous memory
        rx-vlan-offload                  - NIC strips VLAN tags
        ntuple-filters                   - Hardware flow steering rules
        receive-hashing                  - RSS hash for queue distribution

        Most offloads are on by default. Only configure if you need to change.
        Use 'ethtool -k <interface>' to see current feature states.

Configuration:
    Settings are read from config files in the config directory, currently
    set to ${config_dir}. Files are parsed in order, with later values
    overriding earlier ones:

      1. default.conf           Global defaults (parsed first)
      2. <interface>.conf       Per-interface overrides (parsed second)

    If no config files exist, the script uses NIC maximum values for ring
    buffers and CPU count for RSS queues. No features are toggled.

Config file format (key=value):

    # Ring buffer sizes (numeric, capped at NIC maximum)
    rx=4096
    tx=4096

    # RSS combined queue count (numeric, capped at NIC maximum)
    combined=16

    # Hardware feature toggles (on/off)
    tso=on
    gro=on
    lro=off

Example:
    $(basename "$0") --interface enp129s0f0
    $(basename "$0") --interface eth0 --config-dir /etc/my-nic-config-dir
EOF
}

parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            -h|--help)
                usage
                exit 0
                ;;
            -c|--config-dir)
                [[ -z "${2:-}" ]] && { error "Config directory is required!"; usage; exit 1; }
                config_dir="${2}"
                shift 2
                ;;
            -i|--interface)
                [[ -z "${2:-}" ]] && { error "Interface name is required!"; usage; exit 1; }
                interface="${2}"
                shift 2
                ;;
            *)
                panic "Unknown argument: ${1}"
                ;;
        esac
    done

    if [[ -z "${interface}" ]]; then
        error "Interface name is required!"
        usage
        exit 1
    fi
}

# Known numeric configuration keys
declare -rg NUMERIC_KEYS="rx tx combined"

# Read config file into associative array. Validates value types based on key.
read_config() {
    local -r config_file="$1"
    local -n config_ref="$2"

    [[ ! -f "${config_file}" ]] && return 0

    local key value
    while IFS='=' read -r key value; do
        # Skip comments and empty lines
        [[ "${key}" =~ ^[[:space:]]*# ]] && continue
        [[ -z "${key}" ]] && continue

        # Trim whitespace from key
        key="${key#"${key%%[![:space:]]*}"}"  # Trim leading
        key="${key%"${key##*[![:space:]]}"}"  # Trim trailing

        # Strip comments from value and trim whitespace
        value="${value%%#*}"
        value="${value#"${value%%[![:space:]]*}"}"  # Trim leading
        value="${value%"${value##*[![:space:]]}"}"  # Trim trailing

        # Validate based on key type
        if [[ " ${NUMERIC_KEYS} " == *" ${key} "* ]]; then
            # Numeric key: must be a positive integer
            if [[ "${value}" =~ ^[0-9]+$ ]] && (( value > 0 )); then
                # shellcheck disable=SC2034
                config_ref["${key}"]="${value}"
            else
                warn "Invalid numeric value '${value}' for ${key} in ${config_file}, skipping"
            fi
        elif [[ "${value}" == "on" || "${value}" == "off" ]]; then
            # Feature toggle: on/off
            # shellcheck disable=SC2034
            config_ref["${key}"]="${value}"
        else
            warn "Invalid value '${value}' for ${key} in ${config_file}, skipping"
        fi
    done < "${config_file}"
}

main() {
    parse_args "${@}"
    check_root

    info "Tuning NIC ${interface}"

    # Load configuration: default.conf first, then interface-specific overrides
    local -A config=()
    if [[ -d "${config_dir}" ]]; then
        read_config "${config_dir}/default.conf" config
        read_config "${config_dir}/${interface}.conf" config
    fi

    # Get NIC maximum ring buffer sizes.
    # The || true prevents pipefail from aborting when grep finds no match;
    # the regex validation below provides a clear error instead.
    local nic_max_rx nic_max_tx
    # shellcheck disable=SC2016
    nic_max_rx="$("${ETHTOOL}" -g "${interface}" \
        | "${GREP}" --extended-regexp --max-count=1 'RX:.*[0-9]+$' \
        | "${AWK}" '{print $2}')" || true
    # shellcheck disable=SC2016
    nic_max_tx="$("${ETHTOOL}" -g "${interface}" \
        | "${GREP}" --extended-regexp --max-count=1 'TX:.*[0-9]+$' \
        | "${AWK}" '{print $2}')" || true

    # Validate that NIC maximum ring sizes are positive integers
    if [[ ! "${nic_max_rx}" =~ ^[1-9][0-9]*$ ]]; then
        panic "Invalid RX ring buffer size returned for ${interface}: '${nic_max_rx}'"
    fi
    if [[ ! "${nic_max_tx}" =~ ^[1-9][0-9]*$ ]]; then
        panic "Invalid TX ring buffer size returned for ${interface}: '${nic_max_tx}'"
    fi

    # Determine target ring buffer sizes: config value capped at NIC max, or NIC max
    local target_rx target_tx
    if [[ -n "${config[rx]:-}" ]]; then
        target_rx=$(( config[rx] < nic_max_rx ? config[rx] : nic_max_rx ))
    else
        target_rx="${nic_max_rx}"
    fi
    if [[ -n "${config[tx]:-}" ]]; then
        target_tx=$(( config[tx] < nic_max_tx ? config[tx] : nic_max_tx ))
    else
        target_tx="${nic_max_tx}"
    fi

    info "Setting ring buffers for ${interface}: RX=${target_rx}, TX=${target_tx}"
    "${ETHTOOL}" -G "${interface}" rx "${target_rx}" tx "${target_tx}" || {
        panic "Failed to set ring buffers for ${interface} (RX=${target_rx}, TX=${target_tx})"
    }

    # Configure RSS combined queues for multi-core packet processing.
    # Combined queues handle both RX and TX per CPU core, distributing packet
    # processing across cores to maximize throughput.
    local nic_max_combined
    # shellcheck disable=SC2016
    nic_max_combined=$("${ETHTOOL}" -l "${interface}" \
        | "${GREP}" --extended-regexp --max-count=1 "Combined:.*[0-9]+" \
        | "${AWK}" '{print $2}') || true

    if [[ -z "${nic_max_combined}" || "${nic_max_combined}" == "n/a" ]]; then
        info "Combined queues not supported for ${interface}, skipping RSS tuning"
    elif [[ ! "${nic_max_combined}" =~ ^[1-9][0-9]*$ ]]; then
        warn "Invalid combined queue count returned for ${interface}: '${nic_max_combined}', skipping RSS tuning"
    else
        local cpu_cores target_combined
        cpu_cores="$("${NPROC}" --all)"
        [[ "${cpu_cores}" =~ ^[1-9][0-9]*$ ]] || panic "Invalid CPU count from nproc: '${cpu_cores}'"

        if [[ -n "${config[combined]:-}" ]]; then
            # Use config value, capped at NIC max
            target_combined=$(( config[combined] < nic_max_combined ? config[combined] : nic_max_combined ))
        else
            # Default: CPU count, capped at NIC max
            target_combined=$(( cpu_cores < nic_max_combined ? cpu_cores : nic_max_combined ))
        fi

        info "Setting combined queues for ${interface}: ${target_combined}"
        "${ETHTOOL}" -L "${interface}" combined "${target_combined}" || {
            warn "Failed to set combined queues for ${interface} to ${target_combined}"
        }
    fi

    # Apply hardware feature toggles (only features explicitly configured)
    local key current_state desired_state
    for key in "${!config[@]}"; do
        desired_state="${config[${key}]}"

        # Skip non-feature keys (numeric settings already handled above)
        [[ " ${NUMERIC_KEYS} " == *" ${key} "* ]] && continue

        # Get current feature state (awk treats key as literal string, not regex)
        # shellcheck disable=SC2016
        current_state="$("${ETHTOOL}" -k "${interface}" 2>/dev/null \
            | "${AWK}" -v key="${key}" '$1 == key":" {print $2}')" || true

        if [[ -z "${current_state}" ]]; then
            warn "Feature '${key}' not found for ${interface}, skipping"
            continue
        fi

        if [[ "${current_state}" != "${desired_state}" ]]; then
            info "Switching ${key} for ${interface}: ${current_state} -> ${desired_state}"
            "${ETHTOOL}" -K "${interface}" "${key}" "${desired_state}" || {
                warn "Failed to switch ${key} for ${interface} from ${current_state} to ${desired_state}"
            }
        fi
    done

    info "Tuning complete for ${interface}"
    exit 0
}

main "${@}"
