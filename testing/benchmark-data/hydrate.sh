#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'EOF'
Usage: hydrate.sh <dataset>...

Downloads benchmark datasets from S3 to ${HOME}/starburst-benchmark-data.

Datasets:
  all                  Download all datasets below
  hive-clickbench      Hive ClickBench hits (snappy, large files)
  hive-tpch-sf30       Hive TPC-H scale factor 30 (decimal, snappy, parquet)
  hive-tpch-sf100      Hive TPC-H scale factor 100 (decimal, snappy, parquet)
  hive-tpcds-sf100     Hive TPC-DS scale factor 100 (snappy, parquet)
  iceberg-clickbench   Iceberg ClickBench hits (snappy, large files)
  iceberg-tpch-sf30    Iceberg TPC-H scale factor 30 (decimal, snappy, parquet)
  iceberg-tpch-sf100   Iceberg TPC-H scale factor 100 (decimal, snappy, parquet)
  iceberg-tpcds-sf100  Iceberg TPC-DS scale factor 100 (snappy, parquet)

Multiple datasets may be specified, e.g.:
  hydrate.sh hive-clickbench hive-tpch-sf30
EOF
}

if [[ $# -eq 0 ]]; then
    usage >&2
    exit 1
fi

want_hive_clickbench=false
want_hive_tpch_sf30=false
want_hive_tpch_sf100=false
want_hive_tpcds_sf100=false
want_iceberg_clickbench=false
want_iceberg_tpch_sf30=false
want_iceberg_tpch_sf100=false
want_iceberg_tpcds_sf100=false

for arg in "$@"; do
    case "${arg}" in
        all)
            want_hive_clickbench=true
            want_hive_tpch_sf30=true
            want_hive_tpch_sf100=true
            want_hive_tpcds_sf100=true
            want_iceberg_clickbench=true
            want_iceberg_tpch_sf30=true
            want_iceberg_tpch_sf100=true
            want_iceberg_tpcds_sf100=true
            ;;
        hive-clickbench)
            want_hive_clickbench=true
            ;;
        hive-tpch-sf30)
            want_hive_tpch_sf30=true
            ;;
        hive-tpch-sf100)
            want_hive_tpch_sf100=true
            ;;
        hive-tpcds-sf100)
            want_hive_tpcds_sf100=true
            ;;
        iceberg-clickbench)
            want_iceberg_clickbench=true
            ;;
        iceberg-tpch-sf30)
            want_iceberg_tpch_sf30=true
            ;;
        iceberg-tpch-sf100)
            want_iceberg_tpch_sf100=true
            ;;
        iceberg-tpcds-sf100)
            want_iceberg_tpcds_sf100=true
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
TPCDS_TABLES=(
    call_center catalog_page catalog_returns catalog_sales
    customer customer_address customer_demographics
    date_dim household_demographics income_band
    inventory item promotion reason ship_mode
    store store_returns store_sales
    time_dim warehouse web_page web_returns web_sales web_site
)

if "${want_hive_clickbench}"; then
    mkdir -p "${DATA_ROOT}/clickbench/hits/"
    aws s3 sync --delete s3://starburst-benchmarks-data/ClickBench/hive/hits_snappy_large_files "${DATA_ROOT}/clickbench/hits/"
fi

if "${want_iceberg_clickbench}"; then
    local_dir="${DATA_ROOT}/iceberg-clickbench/tables/hits-597fe8e4c83a44ea8999a5703ebd9c1c"
    mkdir -p "${local_dir}/"
    aws s3 sync --delete "s3://starburst-benchmarks-data/ClickBench/iceberg/hits_snappy_large_files-597fe8e4c83a44ea8999a5703ebd9c1c/" "${local_dir}/"
fi

sync_dataset() {
    local source_prefix="$1"
    local target_dir="$2"
    shift 2
    mkdir -p "${target_dir}"
    for table in "$@"; do
        mkdir -p "${target_dir}/${table}/"
        aws s3 sync --delete "${source_prefix}${table}/" "${target_dir}/${table}/"
    done
}

if "${want_hive_tpch_sf30}"; then
    sync_dataset s3://starburst-benchmarks-data/tpch-sf30-dec-snappy-PARQUET/ "${DATA_ROOT}/tpch-sf30" "${TPCH_TABLES[@]}"
fi

if "${want_hive_tpch_sf100}"; then
    sync_dataset s3://starburst-benchmarks-data/tpch-sf100-dec-snappy-PARQUET/ "${DATA_ROOT}/tpch-sf100" "${TPCH_TABLES[@]}"
fi

if "${want_hive_tpcds_sf100}"; then
    # TODO drop the -v20260528 suffix, replace old tpcds-sf100-snappy-PARQUET dataset
    sync_dataset s3://starburst-benchmarks-data/tpcds-sf100-snappy-PARQUET-v20260528/ "${DATA_ROOT}/tpcds-sf100" "${TPCDS_TABLES[@]}"
fi

if "${want_iceberg_tpch_sf30}"; then
    tables=(
        customer-deb77f6e7ac64ac49be8806cd9f795b7
        lineitem-0e2da0fa760040d4b1ff04da71924bba
        nation-dc9e1d142aa840bdad5e2fc7ccb4a887
        orders-e0bce2fbd20f49a5be1240076de42f58
        part-8bff82460e214a10b00b90a29cb105ff
        partsupp-60e773e49a6d499da12323f8c550264d
        region-73e9211f9ddf467eb4a074d59ff6ecc8
        supplier-ae961b8cec9b4dfba2bde399e980121e
    )
    sync_dataset s3://starburst-benchmarks-data/iceberg-tpch-sf30-snappy-PARQUET/ "${DATA_ROOT}/iceberg-tpch-sf30/tables" "${tables[@]}"
fi

if "${want_iceberg_tpch_sf100}"; then
    tpch_tables=(
        customer-f1db0087172d4e90a7eab0f5cde4774f
        lineitem-c3e45c03224643b3a63c478aaae57b08
        nation-f54e78db9ef64cc58bf83431ab13b67e
        orders-16331d4eeedf4e0c993faceafff8b2e3
        part-564b7204508e42de87afa69704a0be7a
        partsupp-befa29c04a7042b89bbf622486705a7c
        region-c8f62a399e354427a2b5eb5dee904da5
        supplier-e61f2fa498264d49b3711c95e38aa619
    )
    sync_dataset s3://starburst-benchmarks-data/iceberg-tpch-sf100-snappy-PARQUET/ "${DATA_ROOT}/iceberg-tpch-sf100/tables" "${tpch_tables[@]}"
fi

if "${want_iceberg_tpcds_sf100}"; then
    tables=(
        call_center-d25f1f5f82b54eb4b952a5be304b7f73
        catalog_page-b6732c62099d4a1c951013c9052045d6
        catalog_returns-256bf8aeece34579bc0f9d19b2cb4543
        catalog_sales-4db5f6164de44048a84ba10c7f17589d
        customer-a274f74636104d6384c3bffa84100cbc
        customer_address-651e1c68f4884fc09f4088c8e436b22c
        customer_demographics-eef2e095d6d14945af22cef0988ed5ff
        date_dim-66a709699883471684cd954c981e4b59
        household_demographics-1b88bf552134431998e69a8e2112fffb
        income_band-b2e7b325f5e640aea9dc400e82f95ba1
        inventory-484b80a5590e449199b01b4ca507ab91
        item-153ad53ffd8948a4ad24ec5020485e4d
        promotion-226aa472cc894da787dfb0682638f10b
        reason-5224765121ef4211952ba66770c4bfe3
        ship_mode-1e424ce99bc64a09b7abbd90a22a2251
        store-1cbeb6c68e2b46f1a61f19fe8527b57a
        store_returns-9f1669793a40476bba696a31a5da1417
        store_sales-c68a9755e2f7494a841fc01770d4dd67
        time_dim-fb84514664e8431eb0884da596ed7a08
        warehouse-bd781304580642779ffa8233b1fe60da
        web_page-ea1d765ebcfb448fa43ccb7e57e8645c
        web_returns-a227bf7382cb46f0827f84d7c4f65b29
        web_sales-28c8ab917b0a49faa78a5bfde99e515b
        web_site-c3dcd5b06e974e2b9b7a7182afd00678
    )
    sync_dataset s3://starburst-benchmarks-data/iceberg-tpcds-sf100-snappy-PARQUET/ "${DATA_ROOT}/iceberg-tpcds-sf100/tables" "${tables[@]}"
fi
