#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'EOF'
Usage: hydrate.sh <dataset>...

Downloads benchmark datasets from S3 to ${HOME}/starburst-benchmark-data.

Datasets:
  all          Download all datasets below
  clickbench   ClickBench hits (hive, snappy, large files)
  tpch-sf30    TPC-H scale factor 30 (decimal, snappy, parquet)
  tpch-sf100   TPC-H scale factor 100 (decimal, snappy, parquet)

Multiple datasets may be specified, e.g.:
  hydrate.sh clickbench tpch-sf30
EOF
}

if [[ $# -eq 0 ]]; then
    usage >&2
    exit 1
fi

want_clickbench=false
want_tpch_sf30=false
want_tpch_sf100=false

for arg in "$@"; do
    case "${arg}" in
        all)
            want_clickbench=true
            want_tpch_sf30=true
            want_tpch_sf100=true
            ;;
        clickbench)
            want_clickbench=true
            ;;
        tpch-sf30)
            want_tpch_sf30=true
            ;;
        tpch-sf100)
            want_tpch_sf100=true
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Error: unrecognized dataset: ${arg}" >&2
            usage >&2
            exit 1
            ;;
    esac
done

set -x

find-aws-profile() {
    local account_id="$1"
    local profile
    local profile_account_id

    for profile in $(aws configure list-profiles); do
        if profile_account_id="$(aws --profile="${profile}" configure get sso_account_id)" &&
            [[ "${profile_account_id}" == "${account_id}" ]]; then
              echo "${profile}"
              return 0
          fi
    done

    echo "Error: No AWS profile found for account ID ${account_id}" >&2
    return 1
}

if test -v AWS_PROFILE; then
    aws_profile_was_set=true
else
    aws_profile_was_set=false
    benchmark_account_id="888469412714"
    profile="$(find-aws-profile "${benchmark_account_id}")"
    echo "Found AWS profile: ${profile} configured for account ID: ${benchmark_account_id}"
    export AWS_PROFILE="${profile}"
fi

if ! aws s3 ls s3://starburst-benchmarks-data/; then
    aws sso login
    if ! aws s3 ls s3://starburst-benchmarks-data/; then
        if "${aws_profile_was_set}"; then
            echo "Unable to access benchmark data. Try running the script with AWS_PROFILE unset." >&2
        else
            echo "Unable to access benchmark data. Please check your AWS client configuration." >&2
        fi
        exit 1
    fi
fi

DATA_ROOT="${HOME}/starburst-benchmark-data"

TPCH_TABLES=(region nation customer supplier part partsupp orders lineitem)

if "${want_clickbench}"; then
    mkdir -p "${DATA_ROOT}/clickbench/hits/"
    aws s3 sync --delete s3://starburst-benchmarks-data/ClickBench/hive/hits_snappy_large_files "${DATA_ROOT}/clickbench/hits/"
fi

sync_tpch() {
    local source_prefix="$1"
    local target_dir="$2"
    mkdir -p "${target_dir}"
    for table in "${TPCH_TABLES[@]}"; do
        mkdir -p "${target_dir}/${table}/"
        aws s3 sync --delete "${source_prefix}${table}/" "${target_dir}/${table}/"
    done
}

if "${want_tpch_sf30}"; then
    sync_tpch s3://starburst-benchmarks-data/tpch-sf30-dec-snappy-PARQUET/ "${DATA_ROOT}/tpch-sf30"
fi

if "${want_tpch_sf100}"; then
    sync_tpch s3://starburst-benchmarks-data/tpch-sf100-dec-snappy-PARQUET/ "${DATA_ROOT}/tpch-sf100"
fi
