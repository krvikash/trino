/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.objectstore;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.iceberg.IcebergPlugin;
import io.trino.spi.session.PropertyMetadata;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.TransactionBuilder.transaction;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class BaseTestObjectStoreProperties
        extends AbstractTestQueryFramework
{
    private static final String CATALOG = "objectstore";

    private final String connectorName;
    private final Map<String, String> objectStoreProperties;

    public BaseTestObjectStoreProperties(String connectorName, Map<String, String> objectStoreProperties)
    {
        this.connectorName = requireNonNull(connectorName, "connectorName is null");
        this.objectStoreProperties = ImmutableMap.copyOf(objectStoreProperties);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        String schema = "default";
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(
                        testSessionBuilder()
                                .setCatalog(CATALOG)
                                .setSchema(schema)
                                .build())
                .build();
        try {
            queryRunner.installPlugin(new IcebergPlugin());
            queryRunner.installPlugin(new ObjectStorePlugin());
            queryRunner.createCatalog(CATALOG, connectorName, objectStoreProperties);

            queryRunner.execute("CREATE SCHEMA %s.%s".formatted(CATALOG, schema));
            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    @Test
    public void testSessionProperties()
    {
        assertThat(query("SHOW SESSION LIKE '" + CATALOG + ".%'"))
                .result()
                .skippingTypesCheck()
                .exceptColumns("Value", "Description")
                // Name, Default, Type
                .matches("""
                        VALUES
                        ('objectstore.bucket_execution_enabled', 'true', 'boolean'),
                        ('objectstore.chunked_line_reader_enabled', 'true', 'boolean'),
                        ('objectstore.collect_column_statistics_on_write', 'true', 'boolean'),
                        ('objectstore.collect_extended_statistics_on_write', 'true', 'boolean'),
                        ('objectstore.columns_to_hide', '[]', 'array(varchar)'),
                        ('objectstore.compression_codec', 'null', 'varchar'),
                        ('objectstore.create_empty_bucket_files', 'false', 'boolean'),
                        ('objectstore.dynamic_filtering_wait_timeout', 'null', 'varchar'),
                        ('objectstore.expire_snapshots_min_retention', '7.00d', 'varchar'),
                        ('objectstore.extended_statistics_collect_on_write', 'true', 'boolean'),
                        ('objectstore.extended_statistics_enabled', 'true', 'boolean'),
                        ('objectstore.force_local_scheduling', 'false', 'boolean'),
                        ('objectstore.hive_storage_format', 'ORC', 'varchar'),
                        ('objectstore.hive_views_legacy_translation', 'false', 'boolean'),
                        ('objectstore.ignore_absent_partitions', 'false', 'boolean'),
                        ('objectstore.ignore_corrupted_statistics', 'false', 'boolean'),
                        ('objectstore.incremental_refresh_enabled', 'true', 'boolean'),
                        ('objectstore.insert_existing_partitions_behavior', 'APPEND', 'varchar'),
                        ('objectstore.max_outstanding_splits', '1000', 'integer'),
                        ('objectstore.max_splits_per_second', '2147483647', 'integer'),
                        ('objectstore.max_partitions_per_writer', '100', 'integer'),
                        ('objectstore.merge_manifests_on_write', 'true', 'boolean'),
                        ('objectstore.minimum_assigned_split_weight', '0.05', 'double'),
                        ('objectstore.non_transactional_optimize_enabled', 'false', 'boolean'),
                        ('objectstore.optimize_mismatched_bucket_count', 'true', 'boolean'),
                        ('objectstore.orc_bloom_filters_enabled', 'false', 'boolean'),
                        ('objectstore.orc_lazy_read_small_ranges', 'true', 'boolean'),
                        ('objectstore.orc_max_buffer_size', '8MB', 'varchar'),
                        ('objectstore.orc_max_merge_distance', '1MB', 'varchar'),
                        ('objectstore.orc_max_read_block_size', '16MB', 'varchar'),
                        ('objectstore.orc_nested_lazy_enabled', 'true', 'boolean'),
                        ('objectstore.orc_optimized_writer_max_dictionary_memory', '16MB', 'varchar'),
                        ('objectstore.orc_optimized_writer_max_stripe_rows', '10000000', 'integer'),
                        ('objectstore.orc_optimized_writer_max_stripe_size', '64MB', 'varchar'),
                        ('objectstore.orc_optimized_writer_min_stripe_size', '32MB', 'varchar'),
                        ('objectstore.orc_optimized_writer_validate', 'false', 'boolean'),
                        ('objectstore.orc_optimized_writer_validate_mode', 'BOTH', 'varchar'),
                        ('objectstore.orc_optimized_writer_validate_percentage', '0.0', 'double'),
                        ('objectstore.orc_stream_buffer_size', '8MB', 'varchar'),
                        ('objectstore.orc_string_statistics_limit', '64B', 'varchar'),
                        ('objectstore.orc_tiny_stripe_threshold', '8MB', 'varchar'),
                        ('objectstore.orc_use_column_names', 'false', 'boolean'),
                        ('objectstore.orc_writer_max_dictionary_memory', '16MB', 'varchar'),
                        ('objectstore.orc_writer_max_stripe_rows', '10000000', 'integer'),
                        ('objectstore.orc_writer_max_stripe_size', '64MB', 'varchar'),
                        ('objectstore.orc_writer_min_stripe_size', '32MB', 'varchar'),
                        ('objectstore.orc_writer_validate_mode', 'BOTH', 'varchar'),
                        ('objectstore.orc_writer_validate_percentage', '0.0', 'double'),
                        ('objectstore.orc_writer_max_row_group_rows', '10000', 'integer'),
                        ('objectstore.parallel_partitioned_bucketed_writes', 'true', 'boolean'),
                        ('objectstore.parquet_ignore_statistics', 'false', 'boolean'),
                        ('objectstore.parquet_max_read_block_row_count', '8192', 'integer'),
                        ('objectstore.parquet_max_read_block_size', '16MB', 'varchar'),
                        ('objectstore.parquet_optimized_writer_validation_percentage', '5.0', 'double'),
                        ('objectstore.parquet_small_file_threshold', '3MB', 'varchar'),
                        ('objectstore.parquet_use_bloom_filter', 'true', 'boolean'),
                        ('objectstore.parquet_use_column_index', 'true', 'boolean'),
                        ('objectstore.parquet_use_column_names', 'true', 'boolean'),
                        ('objectstore.parquet_vectorized_decoding_enabled', 'true', 'boolean'),
                        ('objectstore.parquet_writer_batch_size', '10000', 'integer'),
                        ('objectstore.parquet_writer_block_size', '128MB', 'varchar'),
                        ('objectstore.parquet_writer_page_size', '1048576B', 'varchar'),
                        ('objectstore.parquet_writer_page_value_count', '60000', 'integer'),
                        ('objectstore.partition_statistics_sample_size', '100', 'integer'),
                        ('objectstore.projection_pushdown_enabled', 'true', 'boolean'),
                        ('objectstore.propagate_table_scan_sorting_properties', 'false', 'boolean'),
                        ('objectstore.query_partition_filter_required', 'false', 'boolean'),
                        ('objectstore.query_partition_filter_required_schemas', '[]', 'array(varchar)'),
                        ('objectstore.rcfile_optimized_writer_validate', 'false', 'boolean'),
                        ('objectstore.remove_orphan_files_min_retention', '7.00d', 'varchar'),
                        ('objectstore.respect_table_format', 'true', 'boolean'),
                        ('objectstore.size_based_split_weights_enabled', 'true', 'boolean'),
                        ('objectstore.sorted_writing_enabled', 'true', 'boolean'),
                        ('objectstore.optimize_partial_topn_enabled', 'true', 'boolean'),
                        ('objectstore.split_generator_parallelism', '4', 'integer'),
                        ('objectstore.standard_split_weight_size', '128MB', 'varchar'),
                        ('objectstore.statistics_enabled', 'true', 'boolean'),
                        ('objectstore.target_max_file_size', '1GB', 'varchar'),
                        ('objectstore.idle_writer_min_file_size', '16MB', 'varchar'),
                        ('objectstore.timestamp_precision', 'MILLISECONDS', 'varchar'),
                        ('objectstore.use_file_size_from_metadata', 'true', 'boolean'),
                        ('objectstore.use_parquet_column_names', 'true', 'boolean'),
                        ('objectstore.vacuum_min_retention', '7.00d', 'varchar'),
                        ('objectstore.validate_bucketing', 'true', 'boolean'),
                        ('objectstore.partition_statistics_collect_on_write', 'false', 'boolean'),
                        ('objectstore.partition_statistics_enabled', 'true', 'boolean')""");
    }

    @Test
    public void testHiddenSessionProperties()
    {
        ObjectStoreConnector objectStoreConnector = transaction(getDistributedQueryRunner().getTransactionManager(), getDistributedQueryRunner().getPlannerContext().getMetadata(), getDistributedQueryRunner().getAccessControl())
                .execute(getSession(), transactionSession -> (ObjectStoreConnector) getDistributedQueryRunner().getCoordinator().getConnector(transactionSession, CATALOG));
        ObjectStoreSessionProperties objectStoreSessionProperties = objectStoreConnector.getInjector().getInstance(ObjectStoreSessionProperties.class);

        assertThat(objectStoreSessionProperties.getSessionProperties().stream()
                .filter(PropertyMetadata::isHidden)
                .map(property -> "%s %s %s".formatted(property.getName(), property.getDefaultValue(), property.getSqlType()))
                .sorted())
                .containsExactly(
                        "delegate_transactional_managed_table_location_to_metastore false boolean",
                        "experimental_split_size null varchar",
                        "max_initial_split_size 32MB varchar",
                        "max_split_size null varchar",
                        "store_table_metadata false boolean");
    }
}
