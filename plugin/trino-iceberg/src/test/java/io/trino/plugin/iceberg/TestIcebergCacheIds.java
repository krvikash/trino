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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.JsonMapperProvider;
import io.airlift.tracing.Tracing;
import io.trino.block.BlockJsonSerde;
import io.trino.filesystem.cache.NoopSplitAffinityProvider;
import io.trino.plugin.base.TypeDeserializer;
import io.trino.plugin.hive.LocationAccessControl;
import io.trino.plugin.hive.metastore.file.FileHiveMetastoreConfig;
import io.trino.plugin.hive.metastore.file.FileHiveMetastoreFactory;
import io.trino.plugin.iceberg.catalog.file.FileMetastoreTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.hms.TrinoHiveCatalogFactory;
import io.trino.plugin.iceberg.delete.DeleteFile;
import io.trino.spi.NodeVersion;
import io.trino.spi.SplitWeight;
import io.trino.spi.block.Block;
import io.trino.spi.cache.CacheTableId;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.ConnectorExpressionEvaluator;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.security.AiModelAccessControl;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static io.trino.hdfs.HdfsTestUtils.HDFS_FILE_SYSTEM_FACTORY;
import static io.trino.metadata.InternalBlockEncodingSerde.TESTING_BLOCK_ENCODING_SERDE;
import static io.trino.plugin.iceberg.ColumnIdentity.primitiveColumnIdentity;
import static io.trino.plugin.iceberg.IcebergTestUtils.FILE_IO_FACTORY;
import static io.trino.plugin.iceberg.IcebergTestUtils.OPTIMIZE_POSITION_DELETES;
import static io.trino.plugin.iceberg.IcebergTestUtils.REMOVE_DANGLING_DELETE_FILES;
import static io.trino.plugin.iceberg.IcebergTestUtils.TABLE_STATISTICS_READER;
import static io.trino.plugin.iceberg.delete.DeletionVectorWriter.UNSUPPORTED_DELETION_VECTOR_WRITER;
import static io.trino.spi.predicate.Domain.singleValue;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_SECOND;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.time.ZoneOffset.UTC;
import static java.util.Map.entry;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static org.apache.iceberg.TestIcebergPartitionStatistics.PARTITION_STATISTICS_WRITER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestIcebergCacheIds
{
    private static final String DATABASE_NAME = "iceberg_cache";
    private IcebergCacheMetadata icebergMetadata;
    private IcebergSplitManager splitManager;
    private File tempDir;
    private static final AtomicInteger nextColumnId = new AtomicInteger(1);

    @BeforeAll
    public void setup()
            throws IOException
    {
        tempDir = Files.createTempDirectory(null).toFile();
        FileMetastoreTableOperationsProvider tableOperationsProvider = new FileMetastoreTableOperationsProvider(HDFS_FILE_SYSTEM_FACTORY, FILE_IO_FACTORY);
        IcebergConfig icebergConfig = new IcebergConfig();
        FileHiveMetastoreFactory metastoreFactory = new FileHiveMetastoreFactory(
                new NodeVersion("test_version"),
                HDFS_FILE_SYSTEM_FACTORY,
                true,
                new FileHiveMetastoreConfig()
                        .setCatalogDirectory(tempDir.toURI().toString())
                        .setMetastoreUser("user"),
                Tracing.noopTracer());
        IcebergMetadataFactory icebergMetadataFactory = new IcebergMetadataFactory(
                LocationAccessControl.ALLOW_ALL,
                AiModelAccessControl.ALLOW_ALL,
                TESTING_TYPE_MANAGER,
                createJsonCodec(CommitTaskData.class),
                new TrinoHiveCatalogFactory(
                        icebergConfig,
                        new CatalogName("iceberg"),
                        metastoreFactory,
                        HDFS_FILE_SYSTEM_FACTORY,
                        FILE_IO_FACTORY,
                        TESTING_TYPE_MANAGER,
                        tableOperationsProvider,
                        new IcebergScheduledMvRefreshConfig(),
                        new NoopWorkScheduler(),
                        new NodeVersion("test_version"),
                        true,
                        newDirectExecutorService()),
                new DefaultIcebergFileSystemFactory(HDFS_FILE_SYSTEM_FACTORY),
                TABLE_STATISTICS_READER,
                new TableStatisticsWriter(new NodeVersion("test-version")),
                PARTITION_STATISTICS_WRITER,
                UNSUPPORTED_DELETION_VECTOR_WRITER,
                OPTIMIZE_POSITION_DELETES,
                REMOVE_DANGLING_DELETE_FILES,
                Optional.of(metastoreFactory),
                newDirectExecutorService(),
                newDirectExecutorService(),
                newDirectExecutorService(),
                newDirectExecutorService(),
                icebergConfig,
                ConnectorExpressionEvaluator.NO_OP);
        icebergMetadata = new IcebergCacheMetadata(
                createJsonCodec(IcebergCacheTableId.class),
                createJsonCodec(IcebergColumnHandle.class));
        splitManager = new IcebergSplitManager(
                new IcebergTransactionManager(icebergMetadataFactory),
                TESTING_TYPE_MANAGER,
                new DefaultIcebergFileSystemFactory(HDFS_FILE_SYSTEM_FACTORY),
                listeningDecorator(newSingleThreadExecutor()),
                newSingleThreadScheduledExecutor(),
                createJsonCodec(IcebergCacheSplitId.class),
                new NoopSplitAffinityProvider(),
                ConnectorExpressionEvaluator.NO_OP);
    }

    @AfterAll
    public void tearDown()
            throws IOException
    {
        if (tempDir != null) {
            deleteRecursively(tempDir.toPath(), ALLOW_INSECURE);
        }
    }

    @Test
    public void testTableId()
    {
        IcebergColumnHandle bigIntColumnHandle = newPrimitiveColumn(BIGINT);
        IcebergColumnHandle timestampColumnHandle = newPrimitiveColumn(TIMESTAMP_TZ_MICROS);
        Optional<String> partitionSpecJson = Optional.of("partitionSpecJson");
        SchemaTableName schemaTableName = new SchemaTableName(DATABASE_NAME, "testing");

        // table id without snapshot id is empty
        assertThat(icebergMetadata.getCacheTableId(
                createIcebergTableHandle(schemaTableName, OptionalLong.empty(), "tableSchemaJson", partitionSpecJson, Set.of(), Optional.empty(), "location")))
                .isEqualTo(Optional.empty());

        // `schemaName` should be part of table id
        assertThat(icebergMetadata.getCacheTableId(
                createIcebergTableHandle(schemaTableName, "tableSchemaJson", partitionSpecJson, Set.of(), Optional.empty(), "location")))
                .isNotEqualTo(icebergMetadata.getCacheTableId(
                        createIcebergTableHandle(new SchemaTableName("different", "testing"), "tableSchemaJson", partitionSpecJson, Set.of(), Optional.empty(), "location")));

        // `tableName` should be part of table id
        assertThat(icebergMetadata.getCacheTableId(
                createIcebergTableHandle(schemaTableName, "tableSchemaJson", partitionSpecJson, Set.of(), Optional.empty(), "location")))
                .isNotEqualTo(icebergMetadata.getCacheTableId(
                        createIcebergTableHandle(new SchemaTableName(DATABASE_NAME, "different"), "tableSchemaJson", partitionSpecJson, Set.of(), Optional.empty(), "location")));

        // `tableSchemaJson`  is not part of table id
        assertThat(icebergMetadata.getCacheTableId(
                createIcebergTableHandle(schemaTableName, "tableSchemaJson", partitionSpecJson, Set.of(), Optional.empty(), "location")))
                .isEqualTo(icebergMetadata.getCacheTableId(
                        createIcebergTableHandle(schemaTableName, "different", partitionSpecJson, Set.of(), Optional.empty(), "location")));

        // `partitionSpecJson` is not part of table id
        assertThat(icebergMetadata.getCacheTableId(
                createIcebergTableHandle(schemaTableName, "tableSchemaJson", partitionSpecJson, Set.of(), Optional.empty(), "location")))
                .isEqualTo(icebergMetadata.getCacheTableId(
                        createIcebergTableHandle(schemaTableName, "tableSchemaJson", Optional.of("different"), Set.of(), Optional.empty(), "location")));

        // `projectedColumns` is not part of table id
        assertThat(icebergMetadata.getCacheTableId(
                createIcebergTableHandle(schemaTableName, "tableSchemaJson", partitionSpecJson, Set.of(bigIntColumnHandle), Optional.empty(), "location")))
                .isEqualTo(icebergMetadata.getCacheTableId(
                        createIcebergTableHandle(schemaTableName, "tableSchemaJson", Optional.of("different"), Set.of(), Optional.empty(), "location")));

        // `nameMappingJson` is not part of table id
        assertThat(icebergMetadata.getCacheTableId(
                createIcebergTableHandle(schemaTableName, "tableSchemaJson", partitionSpecJson, Set.of(), Optional.empty(), "location")))
                .isEqualTo(icebergMetadata.getCacheTableId(
                        createIcebergTableHandle(schemaTableName, "tableSchemaJson", Optional.of("different"), Set.of(), Optional.of("different"), "location")));

        // `location` should be part of table id
        assertThat(icebergMetadata.getCacheTableId(
                createIcebergTableHandle(schemaTableName, "tableSchemaJson", partitionSpecJson, Set.of(), Optional.empty(), "location")))
                .isNotEqualTo(icebergMetadata.getCacheTableId(
                        createIcebergTableHandle(schemaTableName, "tableSchemaJson", partitionSpecJson, Set.of(), Optional.empty(), "different")));

        // unenforce predicate should not be part of table id
        assertThat(icebergMetadata.getCacheTableId(createIcebergTableHandle(schemaTableName, "tableSchemaJson", partitionSpecJson, TupleDomain.withColumnDomains(ImmutableMap.of(bigIntColumnHandle, singleValue(BIGINT, 1L))), TupleDomain.all(), Set.of(), Optional.empty(), "location")))
                .isEqualTo(icebergMetadata.getCacheTableId(
                        createIcebergTableHandle(schemaTableName, "tableSchemaJson", partitionSpecJson, Set.of(), Optional.empty(), "location")));

        // unenforce predicate timestamp(6) should be part of table id
        LocalDate someDate = LocalDate.of(2022, 3, 22);

        long startOfDateUtcEpochMillis = someDate.atStartOfDay().toEpochSecond(UTC) * MILLISECONDS_PER_SECOND;
        LongTimestampWithTimeZone startOfDateUtc = timestampTzFromEpochMillis(startOfDateUtcEpochMillis);
        LongTimestampWithTimeZone startOfNextDateUtc = timestampTzFromEpochMillis(startOfDateUtcEpochMillis + MILLISECONDS_PER_DAY);
        assertThat(icebergMetadata.getCacheTableId(createIcebergTableHandle(
                schemaTableName,
                "tableSchemaJson",
                partitionSpecJson,
                TupleDomain.withColumnDomains(Map.of(timestampColumnHandle, Domain.create(ValueSet.ofRanges(Range.range(TIMESTAMP_TZ_MICROS, startOfDateUtc, true, startOfNextDateUtc, false)), false))),
                TupleDomain.all(),
                Set.of(),
                Optional.empty(),
                "location")))
                .isEqualTo(Optional.of(new CacheTableId("{\"schemaName\":\"iceberg_cache\",\"tableName\":\"testing\",\"tableLocation\":\"location\",\"storageProperties\":{}}")));

        // enforce predicate is not part of table id
        assertThat(icebergMetadata.getCacheTableId(
                createIcebergTableHandle(schemaTableName, "tableSchemaJson", partitionSpecJson, TupleDomain.all(), TupleDomain.withColumnDomains(ImmutableMap.of(bigIntColumnHandle, singleValue(BIGINT, 1L))), Set.of(), Optional.empty(), "location")))
                .isEqualTo(icebergMetadata.getCacheTableId(
                        createIcebergTableHandle(schemaTableName, "tableSchemaJson", partitionSpecJson, Set.of(), Optional.empty(), "location")));

        // storage options is part of table id
        assertThat(icebergMetadata.getCacheTableId(
                createIcebergTableHandle(schemaTableName, Map.of("read.split.target-size", "1"))))
                .isNotEqualTo(icebergMetadata.getCacheTableId(
                        createIcebergTableHandle(schemaTableName, "tableSchemaJson", partitionSpecJson, Set.of(), Optional.empty(), "location")));

        // statistics in storage options support was dropped in https://github.com/trinodb/trino/pull/19803, so it is part of table id if exists
        assertThat(icebergMetadata.getCacheTableId(
                createIcebergTableHandle(schemaTableName, Map.of("trino.stats.ndv.1231.ndv", "111", "other", "other"))))
                .isEqualTo(icebergMetadata.getCacheTableId(
                        createIcebergTableHandle(schemaTableName, Map.of("trino.stats.ndv.1231.ndv", "111", "other", "other"))));
    }

    @Test
    public void testSplitId()
    {
        int unpartitionedPartitionSpecJson = 1;
        List<Block> unpartitionedValues = ImmutableList.of();
        assertThat(splitManager.getCacheSplitId(createIcebergSplit("path", 0, 10, 10, IcebergFileFormat.ORC, unpartitionedPartitionSpecJson, unpartitionedValues, List.of())))
                .isEqualTo(splitManager.getCacheSplitId(createIcebergSplit("path", 0, 10, 10, IcebergFileFormat.ORC, unpartitionedPartitionSpecJson, unpartitionedValues, List.of())));

        // different path should make ids different
        assertThat(splitManager.getCacheSplitId(createIcebergSplit("path1", 0, 10, 10, IcebergFileFormat.ORC, unpartitionedPartitionSpecJson, unpartitionedValues, List.of())))
                .isNotEqualTo(splitManager.getCacheSplitId(createIcebergSplit("path2", 0, 10, 10, IcebergFileFormat.ORC, unpartitionedPartitionSpecJson, unpartitionedValues, List.of())));

        // different start position should make ids different
        assertThat(splitManager.getCacheSplitId(createIcebergSplit("path", 0, 10, 10, IcebergFileFormat.ORC, unpartitionedPartitionSpecJson, unpartitionedValues, List.of())))
                .isNotEqualTo(splitManager.getCacheSplitId(createIcebergSplit("path", 10, 10, 10, IcebergFileFormat.ORC, unpartitionedPartitionSpecJson, unpartitionedValues, List.of())));

        // different length should make ids different
        assertThat(splitManager.getCacheSplitId(createIcebergSplit("path", 0, 10, 10, IcebergFileFormat.ORC, unpartitionedPartitionSpecJson, unpartitionedValues, List.of())))
                .isNotEqualTo(splitManager.getCacheSplitId(createIcebergSplit("path", 0, 20, 10, IcebergFileFormat.ORC, unpartitionedPartitionSpecJson, unpartitionedValues, List.of())));

        // different fileSize should make ids different
        assertThat(splitManager.getCacheSplitId(createIcebergSplit("path", 0, 10, 10, IcebergFileFormat.ORC, unpartitionedPartitionSpecJson, unpartitionedValues, List.of())))
                .isNotEqualTo(splitManager.getCacheSplitId(createIcebergSplit("path", 0, 10, 100, IcebergFileFormat.ORC, unpartitionedPartitionSpecJson, unpartitionedValues, List.of())));

        // different file format should make ids different
        assertThat(splitManager.getCacheSplitId(createIcebergSplit("path", 0, 10, 10, IcebergFileFormat.ORC, unpartitionedPartitionSpecJson, unpartitionedValues, List.of())))
                .isNotEqualTo(splitManager.getCacheSplitId(createIcebergSplit("path", 0, 10, 100, IcebergFileFormat.PARQUET, unpartitionedPartitionSpecJson, unpartitionedValues, List.of())));

        // different spec id should make ids different
        int partitionSpecId1 = 1;
        int partitionSpecId2 = 2;
        assertThat(splitManager.getCacheSplitId(createIcebergSplit("path", 0, 10, 10, IcebergFileFormat.ORC, partitionSpecId1, unpartitionedValues, List.of())))
                .isNotEqualTo(splitManager.getCacheSplitId(createIcebergSplit("path", 0, 10, 100, IcebergFileFormat.PARQUET, partitionSpecId2, unpartitionedValues, List.of())));
    }

    @Test
    public void testCacheableStorageProperty()
    {
        assertThat(IcebergCacheTableId.isCacheableStorageProperty(entry("not-cacheable", "test"))).isFalse();
        assertThat(IcebergCacheTableId.isCacheableStorageProperty(entry("trino.stats.ndv.1231.ndv", "test"))).isFalse();
        assertThat(IcebergCacheTableId.isCacheableStorageProperty(entry("fileloader.enabled", "test"))).isFalse();
        assertThat(IcebergCacheTableId.isCacheableStorageProperty(entry("read.parquet.vectorization.batch-size", "test"))).isTrue();
        assertThat(IcebergCacheTableId.isCacheableStorageProperty(entry("read.split.target-size", "test"))).isTrue();
    }

    private static IcebergSplit createIcebergSplit(
            String path,
            long start,
            long length,
            long fileSize,
            IcebergFileFormat fileFormat,
            int specId,
            List<Block> partitionValues,
            List<DeleteFile> deletes)
    {
        return new IcebergSplit(
                path,
                start,
                length,
                fileSize,
                0L,
                fileFormat,
                specId,
                0,
                partitionValues,
                deletes,
                SplitWeight.standard(),
                TupleDomain.all(),
                Optional.empty(),
                OptionalLong.empty(),
                OptionalLong.empty());
    }

    private static IcebergTableHandle createIcebergTableHandle(
            SchemaTableName schemaTableName,
            OptionalLong snapshotId,
            String tableSchemaJson,
            Optional<String> partitionSpecJson,
            Set<IcebergColumnHandle> projectedColumns,
            Optional<String> nameMappingJson,
            String tableLocation)
    {
        return new IcebergTableHandle(
                schemaTableName.getSchemaName(),
                schemaTableName.getTableName(),
                TableType.DATA,
                snapshotId,
                tableSchemaJson,
                partitionSpecJson.isPresent() ? OptionalInt.of(1) : OptionalInt.empty(),
                partitionSpecJson.map(spec -> ImmutableMap.of(1, spec)).orElse(ImmutableMap.of()),
                2,
                TupleDomain.all(),
                TupleDomain.all(),
                OptionalLong.empty(),
                false,
                OptionalInt.empty(),
                projectedColumns,
                nameMappingJson,
                tableLocation,
                Map.of(),
                Optional.empty(),
                Optional.empty(),
                true,
                Optional.empty(),
                false,
                ImmutableSet.of(),
                Optional.empty());
    }

    private static IcebergTableHandle createIcebergTableHandle(
            SchemaTableName schemaTableName,
            String tableSchemaJson,
            Optional<String> partitionSpecJson,
            Set<IcebergColumnHandle> projectedColumns,
            Optional<String> nameMappingJson,
            String tableLocation)
    {
        return new IcebergTableHandle(
                schemaTableName.getSchemaName(),
                schemaTableName.getTableName(),
                TableType.DATA,
                OptionalLong.of(1L),
                tableSchemaJson,
                partitionSpecJson.isPresent() ? OptionalInt.of(1) : OptionalInt.empty(),
                partitionSpecJson.map(spec -> ImmutableMap.of(1, spec)).orElse(ImmutableMap.of()),
                2,
                TupleDomain.all(),
                TupleDomain.all(),
                OptionalLong.empty(),
                false,
                OptionalInt.empty(),
                projectedColumns,
                nameMappingJson,
                tableLocation,
                Map.of(),
                Optional.empty(),
                Optional.empty(),
                true,
                Optional.empty(),
                false,
                ImmutableSet.of(),
                Optional.empty());
    }

    private static IcebergTableHandle createIcebergTableHandle(
            SchemaTableName schemaTableName,
            String tableSchemaJson,
            Optional<String> partitionSpecJson,
            TupleDomain<IcebergColumnHandle> unenforcedPredicate,
            TupleDomain<IcebergColumnHandle> enforcedPredicate,
            Set<IcebergColumnHandle> projectedColumns,
            Optional<String> nameMappingJson,
            String tableLocation)
    {
        return new IcebergTableHandle(
                schemaTableName.getSchemaName(),
                schemaTableName.getTableName(),
                TableType.DATA,
                OptionalLong.of(1L),
                tableSchemaJson,
                partitionSpecJson.isPresent() ? OptionalInt.of(1) : OptionalInt.empty(),
                partitionSpecJson.map(spec -> ImmutableMap.of(1, spec)).orElse(ImmutableMap.of()),
                2,
                unenforcedPredicate,
                enforcedPredicate,
                OptionalLong.empty(),
                false,
                OptionalInt.empty(),
                projectedColumns,
                nameMappingJson,
                tableLocation,
                Map.of(),
                Optional.empty(),
                Optional.empty(),
                true,
                Optional.empty(),
                false,
                ImmutableSet.of(),
                Optional.empty());
    }

    private static IcebergTableHandle createIcebergTableHandle(
            SchemaTableName schemaTableName,
            Map<String, String> storageProperties)
    {
        return new IcebergTableHandle(
                schemaTableName.getSchemaName(),
                schemaTableName.getTableName(),
                TableType.DATA,
                OptionalLong.of(1L),
                "tableSchemaJson",
                OptionalInt.empty(),
                ImmutableMap.of(),
                2,
                TupleDomain.all(),
                TupleDomain.all(),
                OptionalLong.empty(),
                false,
                OptionalInt.empty(),
                Set.of(),
                Optional.empty(),
                "tableLocation",
                storageProperties,
                Optional.empty(),
                Optional.empty(),
                true,
                Optional.empty(),
                false,
                ImmutableSet.of(),
                Optional.empty());
    }

    private static LongTimestampWithTimeZone timestampTzFromEpochMillis(long epochMillis)
    {
        return LongTimestampWithTimeZone.fromEpochMillisAndFraction(epochMillis, 0, UTC_KEY);
    }

    private static IcebergColumnHandle newPrimitiveColumn(Type type)
    {
        int id = nextColumnId.getAndIncrement();
        return IcebergColumnHandle.optional(primitiveColumnIdentity(id, "column_" + id))
                .columnType(type)
                .build();
    }

    public static <T> JsonCodec<T> createJsonCodec(Class<T> clazz)
    {
        JsonMapperProvider jsonMapperProvider = new JsonMapperProvider();
        TypeDeserializer typeDeserializer = new TypeDeserializer(TESTING_TYPE_MANAGER);
        jsonMapperProvider.setJsonDeserializers(
                ImmutableMap.of(
                        Block.class, new BlockJsonSerde.Deserializer(TESTING_BLOCK_ENCODING_SERDE),
                        Type.class, typeDeserializer));
        jsonMapperProvider.setJsonSerializers(ImmutableMap.of(Block.class, new BlockJsonSerde.Serializer(TESTING_BLOCK_ENCODING_SERDE)));
        return new JsonCodecFactory(jsonMapperProvider).jsonCodec(clazz);
    }
}
