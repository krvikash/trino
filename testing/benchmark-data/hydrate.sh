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
    local_dir="${DATA_ROOT}/iceberg-clickbench/tables/hits"
    mkdir -p "${local_dir}/"
    aws s3 sync --delete "s3://baas-benchmark-data/clickbench/iceberg/hits/" "${local_dir}/"
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
        customer-4d33c779befa4572adf9456509446417
        lineitem-55afd99516654c80a764dad11c3869d8
        nation-f18221523a134c779438d63aeeff8c9d
        orders-8ed77cfa918b4ab89ea7547c81d4c4d4
        part-3d2ec790b47e45f991b3b2b68f393727
        partsupp-560dc06ebfcc40798253e594b6387a67
        region-57ac5ea65e314fe28d410437c08781da
        supplier-c04975f381e749eb8381a19c96174a26
    )
    sync_dataset s3://starburst-benchmarks-data/iceberg-tpch-sf30-parquet/ "${DATA_ROOT}/iceberg-tpch-sf30/tables" "${tables[@]}"
fi

if "${want_iceberg_tpch_sf100}"; then
    tpch_tables=(
        customer-8826fb2be8434f459a80851a21ce6a48
        lineitem-5817376144ae4e569d71eb95f6867db2
        nation-22ecbf33f3d64878ad7ef9fcb738c5a3
        orders-657a5880542a4d48a5790660fe85bd46
        part-fba8936441f343f09c999254ec10a676
        partsupp-97b9c1cd18f34aa5bc7574e35e6a77e3
        region-74f93ec72c87444992b69b67aa125bd1
        supplier-72a65d865ba241cf9541831ff8e18fb5
    )
    sync_dataset s3://starburst-benchmarks-data/iceberg-tpch-sf100-parquet/ "${DATA_ROOT}/iceberg-tpch-sf100/tables" "${tpch_tables[@]}"
fi

if "${want_iceberg_tpcds_sf100}"; then
    tables=(
        call_center-cace8ff8957e41659bd77f8deae79ed6
        catalog_page-abf2b37a4b2d457ea75378295c39fa02
        catalog_returns-b374fe5dc5b4463693183f386c251474
        catalog_sales-7131536b11774201b7a9b0d87fb7d07b
        customer-3f927ff1d32d4935aed0180e48f74019
        customer_address-c3be8509ad5d497aa3d6d18fc72d09cd
        customer_demographics-b155cd14884349a4a15b299f4e6cad95
        date_dim-23d031cec1644a1c8123d56c8a4eafbd
        household_demographics-52b544018f094b5b8b925050feba174e
        income_band-5b08ee2498c54dd1a19d4a089893d912
        inventory-04a35aa569bd439cad7cb616a9abde08
        item-06036328e0fc4ea9af861e8eccb8a4d0
        promotion-1c49844df9ae466080df3a48d07e30fe
        reason-9a14387bef2b4d028b1d1dd987a9328e
        ship_mode-779915a8bd2146d39eb94d67ebcc9af8
        store-347a9a4c5a194e3d97066fb362f9d087
        store_returns-ab090d465aaa4eefbe2895aee98c4fcb
        store_sales-dbf6ed25869c4fb08651af77f50a9a07
        time_dim-1a224b45329540558d44b3790118b562
        warehouse-900596e26c504434b5d77a5c599a70a2
        web_page-03c886c64249498e97a74f62404be20d
        web_returns-7c46da8beb874de5898a39d9b68bd465
        web_sales-9485b1bc457a475299e2e43e01c5fa76
        web_site-3701731cfd7045c2bfad58de0a6f1450
    )
    sync_dataset s3://starburst-benchmarks-data/iceberg-tpcds-sf100-parquet/ "${DATA_ROOT}/iceberg-tpcds-sf100/tables" "${tables[@]}"
fi
