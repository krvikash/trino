/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.functions.io;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.plugin.hive.HiveSessionProperties;
import io.trino.spi.session.PropertyMetadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class StorageSessionProperties
        implements SessionPropertiesProvider
{
    private static final Map<String, Boolean> HIVE_SESSION_PROPERTIES_EXPOSURE = ImmutableMap.<String, Boolean>builder()
            .put("bucket_execution_enabled", false)
            .put("validate_bucketing", false)
            .put("target_max_file_size", true)
            .put("idle_writer_min_file_size", true)
            .put("parallel_partitioned_bucketed_writes", false)
            .put("force_local_scheduling", false)
            .put("insert_existing_partitions_behavior", false)
            .put("orc_bloom_filters_enabled", true)
            .put("orc_max_merge_distance", true)
            .put("orc_max_buffer_size", true)
            .put("orc_stream_buffer_size", true)
            .put("orc_tiny_stripe_threshold", true)
            .put("orc_max_read_block_size", true)
            .put("orc_lazy_read_small_ranges", true)
            .put("orc_nested_lazy_enabled", true)
            .put("orc_string_statistics_limit", true)
            .put("orc_optimized_writer_validate", true)
            .put("orc_optimized_writer_validate_percentage", true)
            .put("orc_optimized_writer_validate_mode", true)
            .put("orc_optimized_writer_min_stripe_size", true)
            .put("orc_optimized_writer_max_stripe_size", true)
            .put("orc_optimized_writer_max_stripe_rows", true)
            .put("orc_optimized_writer_max_dictionary_memory", true)
            .put("orc_use_column_names", true)
            .put("chunked_line_reader_enabled", true)
            .put("hive_storage_format", false)
            .put("compression_codec", true)
            .put("respect_table_format", false)
            .put("create_empty_bucket_files", false)
            .put("parquet_use_column_names", true)
            .put("parquet_ignore_statistics", true)
            .put("parquet_use_column_index", true)
            .put("parquet_use_bloom_filter", true)
            .put("parquet_max_read_block_size", true)
            .put("parquet_max_read_block_row_count", true)
            .put("parquet_small_file_threshold", true)
            .put("parquet_vectorized_decoding_enabled", true)
            .put("parquet_writer_block_size", true)
            .put("parquet_writer_page_size", true)
            .put("parquet_writer_page_value_count", true)
            .put("parquet_writer_row_group_max_row_count", true)
            .put("parquet_writer_row_group_size", true)
            .put("parquet_writer_batch_size", true)
            .put("parquet_optimized_writer_validation_percentage", true)
            .put("max_split_size", true)
            .put("rcfile_optimized_writer_validate", true)
            .put("sorted_writing_enabled", true)
            .put("propagate_table_scan_sorting_properties", false)
            .put("statistics_enabled", false)
            .put("partition_statistics_sample_size", false)
            .put("ignore_corrupted_statistics", false)
            .put("collect_column_statistics_on_write", false)
            .put("optimize_mismatched_bucket_count", false)
            .put("delegate_transactional_managed_table_location_to_metastore", false)
            .put("ignore_absent_partitions", false)
            .put("query_partition_filter_required", false)
            .put("query_partition_filter_required_schemas", false)
            .put("projection_pushdown_enabled", true)
            .put("timestamp_precision", true)
            .put("dynamic_filtering_wait_timeout", false)
            .put("hive_views_legacy_translation", false)
            .put("iceberg_catalog_name", false)
            .put("delta_lake_catalog_name", false)
            .put("hudi_catalog_name", false)
            .put("size_based_split_weights_enabled", false)
            .put("minimum_assigned_split_weight", false)
            .put("non_transactional_optimize_enabled", false)
            .buildOrThrow();

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public StorageSessionProperties(HiveSessionProperties hiveSessionProperties)
    {
        Map<String, Boolean> sessionPropertiesExposures = new HashMap<>(HIVE_SESSION_PROPERTIES_EXPOSURE);
        ImmutableList.Builder<PropertyMetadata<?>> delegatePropertiesBuilder = ImmutableList.builder();
        for (PropertyMetadata<?> property : hiveSessionProperties.getSessionProperties()) {
            String name = property.getName();
            boolean exposed = Optional.ofNullable(sessionPropertiesExposures.remove(name))
                    .orElseThrow(() -> new IllegalStateException("Unknown session property provided: %s".formatted(name)));
            if (exposed) {
                delegatePropertiesBuilder.add(property);
            }
        }
        sessionProperties = delegatePropertiesBuilder.build();
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }
}
