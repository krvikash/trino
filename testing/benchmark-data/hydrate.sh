#!/usr/bin/env bash
set -euo pipefail
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

mkdir -p "${DATA_ROOT}/clickbench/hits/"
aws s3 sync --delete s3://starburst-benchmarks-data/ClickBench/hive/hits_snappy_large_files "${DATA_ROOT}/clickbench/hits/"

mkdir -p "${DATA_ROOT}/tpch-sf30/"
aws s3 sync --delete s3://starburst-benchmarks-data/tpch-sf30-dec-snappy-PARQUET/ "${DATA_ROOT}/tpch-sf30/"

# TODO implement download for TPCH sf100 dataset.
# Per https://starburstdata.slack.com/archives/C0AGLT64T8W/p1777627578973239?thread_ts=1777323472.877859&cid=C0AGLT64T8W current download
# contains a lot (around 62 GB) of unnecessary data, which you unlikely want to download.
#mkdir -p "${DATA_ROOT}/tpch-sf100/"
#aws s3 sync --delete s3://starburst-benchmarks-data/tpch-sf100-dec-snappy-PARQUET/ "${DATA_ROOT}/tpch-sf100/"
echo "Download of tpch-sf100 dataset is currently disabled." >&2
exit 1
