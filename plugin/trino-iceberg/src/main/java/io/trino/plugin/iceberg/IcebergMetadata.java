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

import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinition;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.parser.CronParser;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Splitter.MapSplitter;
import com.google.common.base.Suppliers;
import com.google.common.base.VerifyException;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.concurrent.MoreFutures;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.starburst.ai.client.EmbeddingType;
import io.trino.cache.NonEvictableCache;
import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.metastore.Column;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.metastore.TableInfo;
import io.trino.plugin.base.classloader.ClassLoaderSafeSystemTable;
import io.trino.plugin.base.filter.UtcConstraintExtractor;
import io.trino.plugin.base.projection.ApplyProjectionUtil;
import io.trino.plugin.base.projection.ApplyProjectionUtil.ProjectedColumnRepresentation;
import io.trino.plugin.base.util.MaybeLazy;
import io.trino.plugin.hive.HiveCompressionCodec;
import io.trino.plugin.hive.HiveStorageFormat;
import io.trino.plugin.hive.LocationAccessControl;
import io.trino.plugin.iceberg.aggregation.DataSketchStateSerializer;
import io.trino.plugin.iceberg.aggregation.IcebergThetaSketchForStats;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.delete.DeletionVectorWriter;
import io.trino.plugin.iceberg.delete.DeletionVectorWriter.DeletionVectorInfo;
import io.trino.plugin.iceberg.delete.OptimizePositionDeletes;
import io.trino.plugin.iceberg.delete.PositionDeleteFiles;
import io.trino.plugin.iceberg.delete.RemoveDanglingDeleteFiles;
import io.trino.plugin.iceberg.delete.RemoveDanglingDeleteFiles.DanglingDeleteFilesResult;
import io.trino.plugin.iceberg.functions.IcebergFunctionProvider;
import io.trino.plugin.iceberg.procedure.IcebergAddFilesFromTableHandle;
import io.trino.plugin.iceberg.procedure.IcebergAddFilesHandle;
import io.trino.plugin.iceberg.procedure.IcebergDropExtendedStatsHandle;
import io.trino.plugin.iceberg.procedure.IcebergExpireSnapshotsHandle;
import io.trino.plugin.iceberg.procedure.IcebergGenerateEmbeddingsHandle;
import io.trino.plugin.iceberg.procedure.IcebergOptimizeHandle;
import io.trino.plugin.iceberg.procedure.IcebergOptimizeManifestsHandle;
import io.trino.plugin.iceberg.procedure.IcebergOptimizePositionDeletesHandle;
import io.trino.plugin.iceberg.procedure.IcebergRemoveDanglingDeleteFilesHandle;
import io.trino.plugin.iceberg.procedure.IcebergRemoveOrphanFilesHandle;
import io.trino.plugin.iceberg.procedure.IcebergRollbackToSnapshotHandle;
import io.trino.plugin.iceberg.procedure.IcebergTableExecuteHandle;
import io.trino.plugin.iceberg.procedure.IcebergTableProcedureId;
import io.trino.plugin.iceberg.procedure.MigrationUtils.RecursiveDirectory;
import io.trino.plugin.iceberg.system.AllManifestsTable;
import io.trino.plugin.iceberg.system.EntriesTable;
import io.trino.plugin.iceberg.system.FilesTable;
import io.trino.plugin.iceberg.system.HistoryTable;
import io.trino.plugin.iceberg.system.ManifestsTable;
import io.trino.plugin.iceberg.system.MetadataLogEntriesTable;
import io.trino.plugin.iceberg.system.PartitionsTable;
import io.trino.plugin.iceberg.system.PropertiesTable;
import io.trino.plugin.iceberg.system.RefsTable;
import io.trino.plugin.iceberg.system.SnapshotsTable;
import io.trino.plugin.iceberg.util.DataFileWithDeleteFiles;
import io.trino.spi.ErrorCode;
import io.trino.spi.RefreshType;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.BeginTableExecuteResult;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ColumnPosition;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorAnalyzeMetadata;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.connector.ConnectorTableExecuteHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableLayout;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.ConnectorWritableTableHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.DiscretePredicates;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.LocalProperty;
import io.trino.spi.connector.MaterializedViewFreshness;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.RelationColumnsMetadata;
import io.trino.spi.connector.RelationCommentMetadata;
import io.trino.spi.connector.RelationType;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.RowChangeParadigm;
import io.trino.spi.connector.SaveMode;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.SortingProperty;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.connector.TableColumnsMetadata;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.connector.UnificationResult;
import io.trino.spi.connector.WriterScalingOptions;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.Variable;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionDependencyDeclaration;
import io.trino.spi.function.FunctionId;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.SchemaFunctionName;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.AiModelAccessControl;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.statistics.ColumnStatisticMetadata;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.statistics.TableStatisticsMetadata;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.objects.Object2IntOpenCustomHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrays;
import it.unimi.dsi.fastutil.objects.ObjectOpenCustomHashSet;
import jakarta.annotation.Nullable;
import org.apache.datasketches.theta.CompactThetaSketch;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.ContentFileParser;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.IcebergManifestUtils;
import org.apache.iceberg.IsolationLevel;
import org.apache.iceberg.ManageSnapshots;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.PartitionStatisticsWriter;
import org.apache.iceberg.ReplaceSortOrder;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.RewriteManifests;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.RowLevelOperationMode;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SnapshotUpdate;
import org.apache.iceberg.SortField;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdatePartitionSpec;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.UpdateStatistics;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.expressions.Term;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.metrics.CommitMetricsResult;
import org.apache.iceberg.metrics.CommitReport;
import org.apache.iceberg.metrics.MetricsReporters;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.JavaHash;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Type.NestedType;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.ListType;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.util.ParallelIterable;
import org.apache.iceberg.util.PartitionUtil;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.base.Verify.verify;
import static com.google.common.base.Verify.verifyNotNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Iterables.size;
import static com.google.common.collect.Maps.transformValues;
import static com.google.common.collect.Sets.difference;
import static io.airlift.units.Duration.ZERO;
import static io.trino.cache.CacheUtils.uncheckedCacheGet;
import static io.trino.cache.SafeCaches.buildNonEvictableCache;
import static io.trino.filesystem.Locations.isS3Tables;
import static io.trino.plugin.base.projection.ApplyProjectionUtil.extractSupportedProjectedColumns;
import static io.trino.plugin.base.projection.ApplyProjectionUtil.replaceWithNewVariables;
import static io.trino.plugin.base.util.ExecutorUtil.processWithAdditionalThreads;
import static io.trino.plugin.base.util.Procedures.checkProcedureArgument;
import static io.trino.plugin.hive.HiveMetadata.TRANSACTIONAL;
import static io.trino.plugin.hive.HiveTimestampPrecision.DEFAULT_PRECISION;
import static io.trino.plugin.hive.ViewReaderUtil.isSomeKindOfAView;
import static io.trino.plugin.hive.util.HiveTypeUtil.getTypeSignature;
import static io.trino.plugin.hive.util.HiveUtil.isDeltaLakeTable;
import static io.trino.plugin.hive.util.HiveUtil.isHudiTable;
import static io.trino.plugin.hive.util.HiveUtil.isIcebergTable;
import static io.trino.plugin.iceberg.ColumnIdentity.createColumnIdentity;
import static io.trino.plugin.iceberg.ExpressionConverter.isConvertibleToIcebergExpression;
import static io.trino.plugin.iceberg.ExpressionConverter.toIcebergExpression;
import static io.trino.plugin.iceberg.IcebergAnalyzeProperties.getColumnNames;
import static io.trino.plugin.iceberg.IcebergColumnHandle.TRINO_MERGE_PARTITION_DATA;
import static io.trino.plugin.iceberg.IcebergColumnHandle.TRINO_MERGE_PARTITION_SPEC_ID;
import static io.trino.plugin.iceberg.IcebergColumnHandle.TRINO_MERGE_ROW_ID;
import static io.trino.plugin.iceberg.IcebergColumnHandle.TRINO_MERGE_ROW_ID_NAME;
import static io.trino.plugin.iceberg.IcebergColumnHandle.TRINO_MERGE_SOURCE_ROW_ID;
import static io.trino.plugin.iceberg.IcebergColumnHandle.fileModifiedTimeColumnHandle;
import static io.trino.plugin.iceberg.IcebergColumnHandle.lastUpdatedSequenceNumberColumnHandle;
import static io.trino.plugin.iceberg.IcebergColumnHandle.partitionColumnHandle;
import static io.trino.plugin.iceberg.IcebergColumnHandle.pathColumnHandle;
import static io.trino.plugin.iceberg.IcebergColumnHandle.rowIdColumnHandle;
import static io.trino.plugin.iceberg.IcebergDefaultValues.formatIcebergDefaultAsSql;
import static io.trino.plugin.iceberg.IcebergDefaultValues.parseDefaultValue;
import static io.trino.plugin.iceberg.IcebergDefaultValues.toIcebergLiteral;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_BAD_DATA;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_CATALOG_ERROR;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_COMMIT_ERROR;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_FILESYSTEM_ERROR;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_INVALID_METADATA;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_MISSING_METADATA;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_UNSUPPORTED_VIEW_DIALECT;
import static io.trino.plugin.iceberg.IcebergFileFormat.ORC;
import static io.trino.plugin.iceberg.IcebergFileFormat.PARQUET;
import static io.trino.plugin.iceberg.IcebergMaterializedViewProperties.REFRESH_SCHEDULE;
import static io.trino.plugin.iceberg.IcebergMetadataColumn.FILE_MODIFIED_TIME;
import static io.trino.plugin.iceberg.IcebergMetadataColumn.FILE_PATH;
import static io.trino.plugin.iceberg.IcebergMetadataColumn.LAST_UPDATED_SEQUENCE_NUMBER;
import static io.trino.plugin.iceberg.IcebergMetadataColumn.PARTITION;
import static io.trino.plugin.iceberg.IcebergMetadataColumn.ROW_ID;
import static io.trino.plugin.iceberg.IcebergMetadataColumn.isMetadataColumnId;
import static io.trino.plugin.iceberg.IcebergPartitionFunction.Transform.BUCKET;
import static io.trino.plugin.iceberg.IcebergSchemaProperties.LOCATION_PROPERTY;
import static io.trino.plugin.iceberg.IcebergSessionProperties.getExpireSnapshotMinRetention;
import static io.trino.plugin.iceberg.IcebergSessionProperties.getHiveCatalogName;
import static io.trino.plugin.iceberg.IcebergSessionProperties.getQueryPartitionFilterRequiredSchemas;
import static io.trino.plugin.iceberg.IcebergSessionProperties.getRemoveOrphanFilesMinRetention;
import static io.trino.plugin.iceberg.IcebergSessionProperties.isBucketExecutionEnabled;
import static io.trino.plugin.iceberg.IcebergSessionProperties.isCollectExtendedStatisticsOnWrite;
import static io.trino.plugin.iceberg.IcebergSessionProperties.isFileBasedConflictDetectionEnabled;
import static io.trino.plugin.iceberg.IcebergSessionProperties.isIncrementalRefreshEnabled;
import static io.trino.plugin.iceberg.IcebergSessionProperties.isMergeManifestsOnWrite;
import static io.trino.plugin.iceberg.IcebergSessionProperties.isProjectionPushdownEnabled;
import static io.trino.plugin.iceberg.IcebergSessionProperties.isQueryPartitionFilterRequired;
import static io.trino.plugin.iceberg.IcebergSessionProperties.isStatisticsEnabled;
import static io.trino.plugin.iceberg.IcebergSessionProperties.isUnsafeSortingPropertiesEnabled;
import static io.trino.plugin.iceberg.IcebergTableName.isDataTable;
import static io.trino.plugin.iceberg.IcebergTableName.isIcebergTableName;
import static io.trino.plugin.iceberg.IcebergTableName.isMaterializedViewStorage;
import static io.trino.plugin.iceberg.IcebergTableName.tableNameFrom;
import static io.trino.plugin.iceberg.IcebergTableProperties.COMPRESSION_CODEC;
import static io.trino.plugin.iceberg.IcebergTableProperties.DATA_LOCATION_PROPERTY;
import static io.trino.plugin.iceberg.IcebergTableProperties.DELETE_AFTER_COMMIT_ENABLED;
import static io.trino.plugin.iceberg.IcebergTableProperties.EXTRA_PROPERTIES_PROPERTY;
import static io.trino.plugin.iceberg.IcebergTableProperties.FILE_FORMAT_PROPERTY;
import static io.trino.plugin.iceberg.IcebergTableProperties.FORMAT_VERSION_PROPERTY;
import static io.trino.plugin.iceberg.IcebergTableProperties.MAX_COMMIT_RETRY;
import static io.trino.plugin.iceberg.IcebergTableProperties.MAX_PREVIOUS_VERSIONS;
import static io.trino.plugin.iceberg.IcebergTableProperties.MERGE_MODE_PROPERTY;
import static io.trino.plugin.iceberg.IcebergTableProperties.OBJECT_STORE_LAYOUT_ENABLED_PROPERTY;
import static io.trino.plugin.iceberg.IcebergTableProperties.ORC_BLOOM_FILTER_COLUMNS_PROPERTY;
import static io.trino.plugin.iceberg.IcebergTableProperties.PARQUET_BLOOM_FILTER_COLUMNS_PROPERTY;
import static io.trino.plugin.iceberg.IcebergTableProperties.PARTITIONING_PROPERTY;
import static io.trino.plugin.iceberg.IcebergTableProperties.SORTED_BY_PROPERTY;
import static io.trino.plugin.iceberg.IcebergTableProperties.getFormatVersion;
import static io.trino.plugin.iceberg.IcebergTableProperties.getPartitioning;
import static io.trino.plugin.iceberg.IcebergTableProperties.getTableLocation;
import static io.trino.plugin.iceberg.IcebergTableProperties.validateCompression;
import static io.trino.plugin.iceberg.IcebergTableProperties.validateFormatVersion;
import static io.trino.plugin.iceberg.IcebergUtil.buildPath;
import static io.trino.plugin.iceberg.IcebergUtil.canEnforceColumnConstraintInSpecs;
import static io.trino.plugin.iceberg.IcebergUtil.checkFormatForProperty;
import static io.trino.plugin.iceberg.IcebergUtil.commit;
import static io.trino.plugin.iceberg.IcebergUtil.createColumnHandle;
import static io.trino.plugin.iceberg.IcebergUtil.deserializePartitionValue;
import static io.trino.plugin.iceberg.IcebergUtil.fileName;
import static io.trino.plugin.iceberg.IcebergUtil.firstSnapshot;
import static io.trino.plugin.iceberg.IcebergUtil.firstSnapshotAfter;
import static io.trino.plugin.iceberg.IcebergUtil.getColumnHandle;
import static io.trino.plugin.iceberg.IcebergUtil.getColumnMetadatas;
import static io.trino.plugin.iceberg.IcebergUtil.getCompressionPropertyName;
import static io.trino.plugin.iceberg.IcebergUtil.getFileFormat;
import static io.trino.plugin.iceberg.IcebergUtil.getHiveCompressionCodec;
import static io.trino.plugin.iceberg.IcebergUtil.getIcebergTableProperties;
import static io.trino.plugin.iceberg.IcebergUtil.getPartitionColumns;
import static io.trino.plugin.iceberg.IcebergUtil.getPartitionKeys;
import static io.trino.plugin.iceberg.IcebergUtil.getPartitionValues;
import static io.trino.plugin.iceberg.IcebergUtil.getProjectedColumns;
import static io.trino.plugin.iceberg.IcebergUtil.getSnapshotIdAsOfTime;
import static io.trino.plugin.iceberg.IcebergUtil.getTopLevelColumns;
import static io.trino.plugin.iceberg.IcebergUtil.newCreateTableTransaction;
import static io.trino.plugin.iceberg.IcebergUtil.readerForManifest;
import static io.trino.plugin.iceberg.IcebergUtil.schemaFromMetadata;
import static io.trino.plugin.iceberg.IcebergUtil.supportsRowLineage;
import static io.trino.plugin.iceberg.IcebergUtil.validateOrcBloomFilterColumns;
import static io.trino.plugin.iceberg.IcebergUtil.validateParquetBloomFilterColumns;
import static io.trino.plugin.iceberg.IcebergUtil.verifyExtraProperties;
import static io.trino.plugin.iceberg.PartitionFields.parsePartitionFields;
import static io.trino.plugin.iceberg.SortFieldUtils.parseSortFields;
import static io.trino.plugin.iceberg.StructLikeWrapperWithFieldIdToIndex.createStructLikeWrapper;
import static io.trino.plugin.iceberg.TableStatisticsReader.readNdvs;
import static io.trino.plugin.iceberg.TableStatisticsWriter.StatsUpdateMode.INCREMENTAL_UPDATE;
import static io.trino.plugin.iceberg.TableStatisticsWriter.StatsUpdateMode.REPLACE;
import static io.trino.plugin.iceberg.TableType.DATA;
import static io.trino.plugin.iceberg.TypeConverter.toIcebergType;
import static io.trino.plugin.iceberg.TypeConverter.toIcebergTypeForNewColumn;
import static io.trino.plugin.iceberg.TypeConverter.toTrinoType;
import static io.trino.plugin.iceberg.procedure.IcebergTableProcedureId.ADD_FILES;
import static io.trino.plugin.iceberg.procedure.IcebergTableProcedureId.ADD_FILES_FROM_TABLE;
import static io.trino.plugin.iceberg.procedure.IcebergTableProcedureId.DROP_EXTENDED_STATS;
import static io.trino.plugin.iceberg.procedure.IcebergTableProcedureId.EXPIRE_SNAPSHOTS;
import static io.trino.plugin.iceberg.procedure.IcebergTableProcedureId.GENERATE_EMBEDDINGS;
import static io.trino.plugin.iceberg.procedure.IcebergTableProcedureId.OPTIMIZE;
import static io.trino.plugin.iceberg.procedure.IcebergTableProcedureId.OPTIMIZE_MANIFESTS;
import static io.trino.plugin.iceberg.procedure.IcebergTableProcedureId.OPTIMIZE_POSITION_DELETES;
import static io.trino.plugin.iceberg.procedure.IcebergTableProcedureId.REMOVE_DANGLING_DELETE_FILES;
import static io.trino.plugin.iceberg.procedure.IcebergTableProcedureId.REMOVE_ORPHAN_FILES;
import static io.trino.plugin.iceberg.procedure.IcebergTableProcedureId.ROLLBACK_TO_SNAPSHOT;
import static io.trino.plugin.iceberg.procedure.MigrationUtils.addFiles;
import static io.trino.plugin.iceberg.procedure.MigrationUtils.addFilesFromTable;
import static io.trino.plugin.iceberg.util.SystemTableUtil.getAllPartitionFields;
import static io.trino.spi.StandardErrorCode.BRANCH_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.COLUMN_ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.COLUMN_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.CONFIGURATION_INVALID;
import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static io.trino.spi.StandardErrorCode.INVALID_ANALYZE_PROPERTY;
import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static io.trino.spi.StandardErrorCode.INVALID_PROCEDURE_ARGUMENT;
import static io.trino.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static io.trino.spi.StandardErrorCode.QUERY_REJECTED;
import static io.trino.spi.StandardErrorCode.TABLE_ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.TYPE_MISMATCH;
import static io.trino.spi.connector.MaterializedViewFreshness.Freshness.FRESH;
import static io.trino.spi.connector.MaterializedViewFreshness.Freshness.FRESH_WITHIN_GRACE_PERIOD;
import static io.trino.spi.connector.MaterializedViewFreshness.Freshness.STALE;
import static io.trino.spi.connector.MaterializedViewFreshness.Freshness.UNKNOWN;
import static io.trino.spi.connector.RetryMode.NO_RETRIES;
import static io.trino.spi.connector.RowChangeParadigm.DELETE_ROW_AND_INSERT_ROW;
import static io.trino.spi.expression.Constant.TRUE;
import static io.trino.spi.predicate.TupleDomain.columnWiseUnion;
import static io.trino.spi.predicate.TupleDomain.withColumnDomains;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_NANOS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Boolean.parseBoolean;
import static java.lang.Math.floorDiv;
import static java.lang.Math.max;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Collections.emptyMap;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.joining;
import static org.apache.iceberg.IcebergManifestUtils.liveEntries;
import static org.apache.iceberg.MetadataTableType.ALL_ENTRIES;
import static org.apache.iceberg.MetadataTableType.ENTRIES;
import static org.apache.iceberg.ReachableFileUtil.metadataFileLocations;
import static org.apache.iceberg.ReachableFileUtil.statisticsFilesLocations;
import static org.apache.iceberg.RowLevelOperationMode.COPY_ON_WRITE;
import static org.apache.iceberg.RowLevelOperationMode.MERGE_ON_READ;
import static org.apache.iceberg.SnapshotSummary.DELETED_RECORDS_PROP;
import static org.apache.iceberg.SnapshotSummary.REMOVED_EQ_DELETES_PROP;
import static org.apache.iceberg.SnapshotSummary.REMOVED_POS_DELETES_PROP;
import static org.apache.iceberg.SnapshotSummary.TOTAL_DATA_FILES_PROP;
import static org.apache.iceberg.SnapshotSummary.TOTAL_DELETE_FILES_PROP;
import static org.apache.iceberg.SnapshotSummary.TOTAL_EQ_DELETES_PROP;
import static org.apache.iceberg.SnapshotSummary.TOTAL_POS_DELETES_PROP;
import static org.apache.iceberg.SnapshotSummary.TOTAL_RECORDS_PROP;
import static org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES;
import static org.apache.iceberg.TableProperties.DELETE_ISOLATION_LEVEL;
import static org.apache.iceberg.TableProperties.DELETE_ISOLATION_LEVEL_DEFAULT;
import static org.apache.iceberg.TableProperties.ENCRYPTION_TABLE_KEY;
import static org.apache.iceberg.TableProperties.FORMAT_VERSION;
import static org.apache.iceberg.TableProperties.MANIFEST_TARGET_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.MANIFEST_TARGET_SIZE_BYTES_DEFAULT;
import static org.apache.iceberg.TableProperties.MERGE_MODE;
import static org.apache.iceberg.TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED;
import static org.apache.iceberg.TableProperties.METADATA_PREVIOUS_VERSIONS_MAX;
import static org.apache.iceberg.TableProperties.MIN_SNAPSHOTS_TO_KEEP;
import static org.apache.iceberg.TableProperties.MIN_SNAPSHOTS_TO_KEEP_DEFAULT;
import static org.apache.iceberg.TableProperties.OBJECT_STORE_ENABLED;
import static org.apache.iceberg.TableProperties.ORC_BLOOM_FILTER_COLUMNS;
import static org.apache.iceberg.TableProperties.PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX;
import static org.apache.iceberg.TableProperties.WRITE_DATA_LOCATION;
import static org.apache.iceberg.TableProperties.WRITE_LOCATION_PROVIDER_IMPL;
import static org.apache.iceberg.TableUtil.formatVersion;
import static org.apache.iceberg.expressions.Expressions.alwaysTrue;
import static org.apache.iceberg.types.TypeUtil.indexParents;
import static org.apache.iceberg.util.LocationUtil.stripTrailingSlash;
import static org.apache.iceberg.util.PropertyUtil.propertyAsInt;
import static org.apache.iceberg.util.SnapshotUtil.schemaFor;
import static org.joda.time.DateTimeZone.UTC;

public class IcebergMetadata
        implements ConnectorMetadata
{
    private static final Logger log = Logger.get(IcebergMetadata.class);
    private static final int TIMESTAMP_NANOS_SUPPORTED_MIN_VERSION = 3;
    private static final int OPTIMIZE_MAX_SUPPORTED_TABLE_VERSION = 3;
    private static final int CLEANING_UP_PROCEDURES_MAX_SUPPORTED_TABLE_VERSION = 3;
    private static final String RETENTION_THRESHOLD = "retention_threshold";
    private static final String UNKNOWN_SNAPSHOT_TOKEN = "UNKNOWN";
    public static final Set<String> UPDATABLE_TABLE_PROPERTIES = ImmutableSet.<String>builder()
            .add(EXTRA_PROPERTIES_PROPERTY)
            .add(FILE_FORMAT_PROPERTY)
            .add(FORMAT_VERSION_PROPERTY)
            .add(COMPRESSION_CODEC)
            .add(MAX_COMMIT_RETRY)
            .add(DELETE_AFTER_COMMIT_ENABLED)
            .add(MAX_PREVIOUS_VERSIONS)
            .add(OBJECT_STORE_LAYOUT_ENABLED_PROPERTY)
            .add(DATA_LOCATION_PROPERTY)
            .add(ORC_BLOOM_FILTER_COLUMNS_PROPERTY)
            .add(PARQUET_BLOOM_FILTER_COLUMNS_PROPERTY)
            .add(PARTITIONING_PROPERTY)
            .add(SORTED_BY_PROPERTY)
            .add(MERGE_MODE_PROPERTY)
            .build();
    public static final Set<String> UPDATABLE_MATERIALIZED_VIEW_PROPERTIES = ImmutableSet.of(REFRESH_SCHEDULE);
    private static final String SYSTEM_SCHEMA = "system";

    public static final String NUMBER_OF_DISTINCT_VALUES_NAME = "NUMBER_OF_DISTINCT_VALUES";
    private static final FunctionName NUMBER_OF_DISTINCT_VALUES_FUNCTION = new FunctionName(IcebergThetaSketchForStats.NAME);

    private static final Integer DELETE_BATCH_SIZE = 1000;
    public static final int GET_METADATA_BATCH_SIZE = 1000;
    private static final MapSplitter MAP_SPLITTER = Splitter.on(",").trimResults().omitEmptyStrings().withKeyValueSeparator("=");
    private static final CronDefinition CRON_DEFINITION = CronDefinitionBuilder.instanceDefinitionFor(CronType.UNIX);
    private static final CronParser CRON_PARSER = new CronParser(CRON_DEFINITION);

    private static final String DEPENDS_ON_TABLES = "dependsOnTables";
    private static final String DEPENDS_ON_TABLE_FUNCTIONS = "dependsOnTableFunctions";
    private static final String DEPENDS_ON_NON_DETERMINISTIC_FUNCTIONS = "dependsOnNonDeterministicFunctions";
    // Value should be ISO-8601 formatted time instant
    private static final String TRINO_QUERY_START_TIME = "trino-query-start-time";

    private final LocationAccessControl locationAccessControl;
    private final AiModelAccessControl aiModelAccessControl;
    private final TypeManager typeManager;
    private final JsonCodec<CommitTaskData> commitTaskCodec;
    private final TrinoCatalog catalog;
    private final IcebergFileSystemFactory fileSystemFactory;
    private final TableStatisticsReader tableStatisticsReader;
    private final TableStatisticsWriter tableStatisticsWriter;
    private final PartitionStatisticsWriter partitionStatisticsWriter;
    private final OptimizePositionDeletes optimizePositionDeletes;
    private final Optional<HiveMetastoreFactory> metastoreFactory;
    private final int maxFormatVersion;
    private final boolean addFilesProcedureEnabled;
    private final Predicate<String> allowedExtraProperties;
    private final DateTimeZone dateTimeZone;
    private final ExecutorService icebergScanExecutor;
    private final Executor metadataFetchingExecutor;
    private final ExecutorService icebergPlanningExecutor;
    private final ExecutorService icebergFileDeleteExecutor;
    private final int materializedViewRefreshMaxSnapshotsToExpire;
    private final Duration materializedViewRefreshSnapshotRetentionPeriod;
    private final Map<IcebergTableHandle, AtomicReference<TableStatistics>> tableStatisticsCache = new ConcurrentHashMap<>();
    private final NonEvictableCache<SchemaTableName, IcebergTableCredentials> tableCredentialsCache;
    private final Map<String, org.apache.iceberg.Metrics> fileMetrics = new HashMap<>();
    private final DeletionVectorWriter deletionVectorWriter;
    private final RemoveDanglingDeleteFiles removeDanglingDeleteFiles;

    private Transaction transaction;
    private OptionalLong fromSnapshotForRefresh = OptionalLong.empty();

    public IcebergMetadata(
            LocationAccessControl locationAccessControl,
            AiModelAccessControl aiModelAccessControl,
            TypeManager typeManager,
            JsonCodec<CommitTaskData> commitTaskCodec,
            TrinoCatalog catalog,
            IcebergFileSystemFactory fileSystemFactory,
            TableStatisticsReader tableStatisticsReader,
            TableStatisticsWriter tableStatisticsWriter,
            PartitionStatisticsWriter partitionStatisticsWriter,
            DeletionVectorWriter deletionVectorWriter,
            OptimizePositionDeletes optimizePositionDeletes,
            RemoveDanglingDeleteFiles removeDanglingDeleteFiles,
            Optional<HiveMetastoreFactory> metastoreFactory,
            int maxFormatVersion,
            boolean addFilesProcedureEnabled,
            Predicate<String> allowedExtraProperties,
            DateTimeZone dateTimeZone,
            ExecutorService icebergScanExecutor,
            Executor metadataFetchingExecutor,
            ExecutorService icebergPlanningExecutor,
            ExecutorService icebergFileDeleteExecutor,
            int materializedViewRefreshMaxSnapshotsToExpire,
            Duration materializedViewRefreshSnapshotRetentionPeriod)
    {
        this.locationAccessControl = requireNonNull(locationAccessControl, "locationAccessControl is null");
        this.aiModelAccessControl = requireNonNull(aiModelAccessControl, "aiModelAccessControl is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.commitTaskCodec = requireNonNull(commitTaskCodec, "commitTaskCodec is null");
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.tableStatisticsReader = requireNonNull(tableStatisticsReader, "tableStatisticsReader is null");
        this.tableStatisticsWriter = requireNonNull(tableStatisticsWriter, "tableStatisticsWriter is null");
        this.partitionStatisticsWriter = requireNonNull(partitionStatisticsWriter, "partitionStatisticsWriter is null");
        this.optimizePositionDeletes = requireNonNull(optimizePositionDeletes, "optimizePositionDeletes is null");
        this.metastoreFactory = requireNonNull(metastoreFactory, "metastoreFactory is null");
        this.maxFormatVersion = maxFormatVersion;
        this.addFilesProcedureEnabled = addFilesProcedureEnabled;
        this.allowedExtraProperties = requireNonNull(allowedExtraProperties, "allowedExtraProperties is null");
        this.dateTimeZone = requireNonNull(dateTimeZone, "dateTimeZone is null");
        this.icebergScanExecutor = requireNonNull(icebergScanExecutor, "icebergScanExecutor is null");
        this.metadataFetchingExecutor = requireNonNull(metadataFetchingExecutor, "metadataFetchingExecutor is null");
        this.icebergPlanningExecutor = requireNonNull(icebergPlanningExecutor, "icebergPlanningExecutor is null");
        this.icebergFileDeleteExecutor = requireNonNull(icebergFileDeleteExecutor, "icebergFileDeleteExecutor is null");
        this.materializedViewRefreshMaxSnapshotsToExpire = materializedViewRefreshMaxSnapshotsToExpire;
        this.materializedViewRefreshSnapshotRetentionPeriod = materializedViewRefreshSnapshotRetentionPeriod;
        this.deletionVectorWriter = requireNonNull(deletionVectorWriter, "deletionVectorWriter is null");
        this.removeDanglingDeleteFiles = requireNonNull(removeDanglingDeleteFiles, "removeDanglingDeleteFiles is null");
        this.tableCredentialsCache = buildNonEvictableCache(CacheBuilder.newBuilder());
    }

    @Override
    public Optional<ConnectorTableCredentials> getTableCredentials(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return getOrLoadTableCredentials(session, getSchemaTableName(tableHandle));
    }

    @Override
    public Optional<ConnectorTableCredentials> getTableCredentials(ConnectorSession session, ConnectorWritableTableHandle tableHandle)
    {
        return getOrLoadTableCredentials(session, getSchemaTableName(tableHandle));
    }

    private Optional<ConnectorTableCredentials> getOrLoadTableCredentials(ConnectorSession session, SchemaTableName schemaTableName)
    {
        try {
            return Optional.of(uncheckedCacheGet(
                    tableCredentialsCache,
                    schemaTableName,
                    () -> {
                        BaseTable baseTable = catalog.loadTable(session, schemaTableName);
                        return new IcebergTableCredentials(baseTable.io().properties());
                    }));
        }
        catch (UncheckedExecutionException e) {
            throwIfUnchecked(e.getCause());
            throw e;
        }
    }

    private static SchemaTableName getSchemaTableName(ConnectorTableHandle tableHandle)
    {
        if (tableHandle instanceof IcebergTableHandle handle) {
            return handle.getSchemaTableName();
        }
        throw new IllegalArgumentException("Unsupported ConnectorTableHandle type: " + tableHandle.getClass().getName());
    }

    private static SchemaTableName getSchemaTableName(ConnectorWritableTableHandle tableHandle)
    {
        return switch (tableHandle) {
            case IcebergTableExecuteHandle handle -> handle.schemaTableName();
            case IcebergWritableTableHandle handle -> handle.name();
            default -> throw new IllegalArgumentException("Unsupported ConnectorWritableTableHandle type: " + tableHandle.getClass().getName());
        };
    }

    @Override
    public Collection<FunctionMetadata> listFunctions(ConnectorSession session, String schemaName)
    {
        return schemaName.equals(SYSTEM_SCHEMA) ? IcebergFunctionProvider.FUNCTIONS : List.of();
    }

    @Override
    public Collection<FunctionMetadata> getFunctions(ConnectorSession session, SchemaFunctionName name)
    {
        if (!name.schemaName().equals(SYSTEM_SCHEMA)) {
            return List.of();
        }
        return IcebergFunctionProvider.FUNCTIONS.stream()
                .filter(function -> function.getCanonicalName().equals(name.functionName()))
                .toList();
    }

    @Override
    public FunctionMetadata getFunctionMetadata(ConnectorSession session, FunctionId functionId)
    {
        return IcebergFunctionProvider.FUNCTIONS.stream()
                .filter(function -> function.getFunctionId().equals(functionId))
                .findFirst()
                .orElseThrow();
    }

    public MaybeLazy<List<ColumnMetadata>> getTableColumnMetadata(ConnectorSession session, io.trino.metastore.Table metastoreTable)
    {
        return catalog.getTableColumnMetadata(session, metastoreTable);
    }

    public MaybeLazy<Optional<String>> getTableComment(ConnectorSession session, io.trino.metastore.Table metastoreTable)
    {
        return catalog.getTableComment(session, metastoreTable);
    }

    private static Optional<String> getTableComment(Table icebergTable)
    {
        return IcebergUtil.getTableComment(icebergTable);
    }

    @Override
    public boolean schemaExists(ConnectorSession session, String schemaName)
    {
        return catalog.namespaceExists(session, schemaName);
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return catalog.listNamespaces(session);
    }

    @Override
    public Map<String, Object> getSchemaProperties(ConnectorSession session, String schemaName)
    {
        return catalog.loadNamespaceMetadata(session, schemaName);
    }

    @Override
    public Optional<TrinoPrincipal> getSchemaOwner(ConnectorSession session, String schemaName)
    {
        return catalog.getNamespacePrincipal(session, schemaName);
    }

    @Override
    public ConnectorTableHandle getTableHandle(
            ConnectorSession session,
            SchemaTableName tableName,
            Optional<ConnectorTableVersion> startVersion,
            Optional<ConnectorTableVersion> endVersion)
    {
        if (startVersion.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "Read table with start version is not supported");
        }

        if (!isIcebergTableName(tableName.getTableName())) {
            return null;
        }

        if (isMaterializedViewStorage(tableName.getTableName())) {
            verify(endVersion.isEmpty(), "Materialized views do not support versioned queries");

            SchemaTableName materializedViewName = new SchemaTableName(tableName.getSchemaName(), tableNameFrom(tableName.getTableName()));
            if (getMaterializedView(session, materializedViewName).isEmpty()) {
                throw new TableNotFoundException(tableName);
            }

            BaseTable storageTable = catalog.getMaterializedViewStorageTable(session, materializedViewName)
                    .orElseThrow(() -> new TrinoException(TABLE_NOT_FOUND, "Storage table metadata not found for materialized view " + tableName));

            return tableHandleForCurrentSnapshot(session, tableName, storageTable, Optional.empty());
        }

        if (!isDataTable(tableName.getTableName())) {
            // Pretend the table does not exist to produce better error message in case of table redirects to Hive
            return null;
        }

        BaseTable table;
        try {
            table = catalog.loadTable(session, new SchemaTableName(tableName.getSchemaName(), tableName.getTableName()));
        }
        catch (TableNotFoundException e) {
            return null;
        }
        catch (TrinoException e) {
            ErrorCode errorCode = e.getErrorCode();
            if (errorCode.equals(ICEBERG_MISSING_METADATA.toErrorCode())
                    || errorCode.equals(ICEBERG_INVALID_METADATA.toErrorCode())) {
                return new CorruptedIcebergTableHandle(tableName, e);
            }
            throw e;
        }

        if (table.properties().containsKey(ENCRYPTION_TABLE_KEY)) {
            // TODO https://starburstdata.atlassian.net/browse/CONNECT-577 Support table encryption
            throw new TrinoException(NOT_SUPPORTED, "Table encryption is not supported for: " + tableName);
        }

        if (endVersion.isPresent()) {
            ConnectorTableVersion version = endVersion.get();
            long snapshotId = getSnapshotIdFromVersion(session, table, version);
            Optional<PartitionSpec> partitionSpec = Optional.empty();

            Optional<String> branch = Optional.empty();
            if (version.getVersionType() instanceof VarcharType) {
                String refName = ((Slice) version.getVersion()).toStringUtf8();
                SnapshotRef ref = table.refs().get(refName);
                if (ref.isBranch()) {
                    branch = Optional.of(refName);
                    partitionSpec = Optional.of(table.spec());
                }
            }

            return tableHandleForSnapshot(
                    session,
                    tableName,
                    table,
                    OptionalLong.of(snapshotId),
                    schemaFor(table, snapshotId),
                    partitionSpec,
                    branch);
        }
        return tableHandleForCurrentSnapshot(session, tableName, table, Optional.empty());
    }

    private static void validateTableForTrino(BaseTable table, OptionalLong tableSnapshotId)
    {
        Snapshot snapshot = tableSnapshotId.isPresent() ? table.snapshot(tableSnapshotId.getAsLong()) : table.currentSnapshot();
        if (snapshot == null) {
            // empty table, nothing to validate
            return;
        }

        TableMetadata metadata = table.operations().current();
        if (metadata.formatVersion() < 3) {
            return;
        }

        // Reject Iceberg table encryption
        if (!metadata.encryptionKeys().isEmpty() || snapshot.keyId() != null || metadata.properties().containsKey("encryption.key-id")) {
            throw new TrinoException(NOT_SUPPORTED, "Iceberg table encryption is not supported");
        }
    }

    private IcebergTableHandle tableHandleForCurrentSnapshot(ConnectorSession session, SchemaTableName tableName, BaseTable table, Optional<String> branch)
    {
        return tableHandleForSnapshot(
                session,
                tableName,
                table,
                getCurrentSnapshotId(table),
                table.schema(),
                Optional.of(table.spec()),
                branch);
    }

    private IcebergTableHandle tableHandleForSnapshot(
            ConnectorSession session,
            SchemaTableName tableName,
            BaseTable table,
            OptionalLong tableSnapshotId,
            Schema tableSchema,
            Optional<PartitionSpec> partitionSpec,
            Optional<String> branch)
    {
        validateTableForTrino(table, tableSnapshotId);
        Map<String, String> tableProperties = table.properties();
        return new IcebergTableHandle(
                tableName.getSchemaName(),
                tableName.getTableName(),
                DATA,
                tableSnapshotId,
                SchemaParser.toJson(tableSchema),
                partitionSpec.map(spec -> OptionalInt.of(spec.specId())).orElseGet(OptionalInt::empty),
                transformValues(table.specs(), PartitionSpecParser::toJson),
                formatVersion(table),
                TupleDomain.all(),
                TupleDomain.all(),
                OptionalLong.empty(),
                false,
                ImmutableSet.of(),
                Optional.ofNullable(tableProperties.get(TableProperties.DEFAULT_NAME_MAPPING)),
                table.location(),
                table.properties(),
                getTablePartitioning(session, table),
                branch,
                false,
                Optional.empty(),
                false,
                ImmutableSet.of(),
                Optional.of(false));
    }

    private Optional<IcebergTablePartitioning> getTablePartitioning(ConnectorSession session, Table icebergTable)
    {
        if (!isBucketExecutionEnabled(session) || icebergTable.specs().size() != 1) {
            return Optional.empty();
        }
        PartitionSpec partitionSpec = icebergTable.spec();
        if (partitionSpec.fields().isEmpty()) {
            return Optional.empty();
        }

        IcebergPartitioningHandle partitioningHandle = IcebergPartitioningHandle.create(partitionSpec, typeManager, List.of());

        Map<Integer, IcebergColumnHandle> partitionColumnById = getPartitionColumns(icebergTable, typeManager).stream()
                .collect(toImmutableMap(IcebergColumnHandle::getId, identity()));
        List<IcebergColumnHandle> partitionColumns = partitionSpec.fields().stream()
                .map(PartitionField::sourceId)
                .distinct()
                .sorted()
                .map(partitionColumnById::get)
                .collect(toImmutableList());

        // Partitioning is only activated if it is actually necessary for the query.
        // This happens in applyPartitioning
        return Optional.of(new IcebergTablePartitioning(
                false,
                partitioningHandle,
                partitionColumns,
                IntStream.range(0, partitioningHandle.partitionFunctions().size()).boxed().collect(toImmutableList())));
    }

    private static long getSnapshotIdFromVersion(ConnectorSession session, Table table, ConnectorTableVersion version)
    {
        io.trino.spi.type.Type versionType = version.getVersionType();
        return switch (version.getPointerType()) {
            case TEMPORAL -> getTemporalSnapshotIdFromVersion(session, table, version, versionType);
            case TARGET_ID -> getTargetSnapshotIdFromVersion(table, version, versionType);
        };
    }

    private static long getTargetSnapshotIdFromVersion(Table table, ConnectorTableVersion version, io.trino.spi.type.Type versionType)
    {
        long snapshotId;
        if (versionType == BIGINT) {
            snapshotId = (long) version.getVersion();
        }
        else if (versionType instanceof VarcharType) {
            String refName = ((Slice) version.getVersion()).toStringUtf8();
            SnapshotRef ref = table.refs().get(refName);
            if (ref == null) {
                throw new TrinoException(INVALID_ARGUMENTS, "Cannot find snapshot with reference name: " + refName);
            }
            snapshotId = ref.snapshotId();
        }
        else {
            throw new TrinoException(NOT_SUPPORTED, "Unsupported type for table version: " + versionType.getDisplayName());
        }

        if (table.snapshot(snapshotId) == null) {
            throw new TrinoException(INVALID_ARGUMENTS, "Iceberg snapshot ID does not exists: " + snapshotId);
        }
        return snapshotId;
    }

    private static long getTemporalSnapshotIdFromVersion(ConnectorSession session, Table table, ConnectorTableVersion version, io.trino.spi.type.Type versionType)
    {
        if (versionType.equals(DATE)) {
            // Retrieve the latest snapshot made before or at the beginning of the day of the specified date in the session's time zone
            long epochMillis = LocalDate.ofEpochDay((Long) version.getVersion())
                    .atStartOfDay()
                    .atZone(session.getTimeZoneKey().getZoneId())
                    .toInstant()
                    .toEpochMilli();
            return getSnapshotIdAsOfTime(table, epochMillis);
        }
        if (versionType instanceof TimestampType timestampVersionType) {
            long epochMicrosUtc = timestampVersionType.isShort()
                    ? (long) version.getVersion()
                    : ((LongTimestamp) version.getVersion()).getEpochMicros();
            long epochMillisUtc = floorDiv(epochMicrosUtc, MICROSECONDS_PER_MILLISECOND);
            long epochMillis = LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMillisUtc), ZoneOffset.UTC)
                    .atZone(session.getTimeZoneKey().getZoneId())
                    .toInstant()
                    .toEpochMilli();
            return getSnapshotIdAsOfTime(table, epochMillis);
        }
        if (versionType instanceof TimestampWithTimeZoneType timeZonedVersionType) {
            long epochMillis = timeZonedVersionType.isShort()
                    ? unpackMillisUtc((long) version.getVersion())
                    : ((LongTimestampWithTimeZone) version.getVersion()).getEpochMillis();
            return getSnapshotIdAsOfTime(table, epochMillis);
        }
        throw new TrinoException(NOT_SUPPORTED, "Unsupported type for temporal table version: " + versionType.getDisplayName());
    }

    @Override
    public Optional<SystemTable> getSystemTable(ConnectorSession session, SchemaTableName tableName)
    {
        return getRawSystemTable(session, tableName)
                .map(systemTable -> new ClassLoaderSafeSystemTable(systemTable, getClass().getClassLoader()));
    }

    private Optional<SystemTable> getRawSystemTable(ConnectorSession session, SchemaTableName tableName)
    {
        if (!isIcebergTableName(tableName.getTableName()) || isDataTable(tableName.getTableName()) || isMaterializedViewStorage(tableName.getTableName())) {
            return Optional.empty();
        }

        // Only when dealing with an actual system table proceed to retrieve the base table for the system table
        String name = tableNameFrom(tableName.getTableName());
        BaseTable table;
        try {
            table = catalog.loadTable(session, new SchemaTableName(tableName.getSchemaName(), name));
        }
        catch (TableNotFoundException e) {
            return Optional.empty();
        }
        catch (UnknownTableTypeException e) {
            // avoid dealing with non Iceberg tables
            return Optional.empty();
        }

        tableCredentialsCache.put(tableName, IcebergTableCredentials.forFileIO(table.io()));

        TableType tableType = IcebergTableName.tableTypeFrom(tableName.getTableName());
        return switch (tableType) {
            case DATA, MATERIALIZED_VIEW_STORAGE, ERRORS -> throw new VerifyException("Unexpected table type: " + tableType); // Handled above.
            case HISTORY -> Optional.of(new HistoryTable(tableName, table));
            case METADATA_LOG_ENTRIES -> Optional.of(new MetadataLogEntriesTable(tableName, table, icebergScanExecutor));
            case SNAPSHOTS -> Optional.of(new SnapshotsTable(tableName, typeManager, table, icebergScanExecutor));
            case PARTITIONS -> Optional.of(new PartitionsTable(tableName, typeManager, table, getCurrentSnapshotId(table), icebergScanExecutor));
            case ALL_MANIFESTS -> Optional.of(new AllManifestsTable(tableName, table, icebergScanExecutor));
            case MANIFESTS -> Optional.of(new ManifestsTable(tableName, table, getCurrentSnapshotId(table)));
            case FILES -> Optional.of(new FilesTable(tableName, typeManager, table, getCurrentSnapshotId(table)));
            case ALL_ENTRIES -> Optional.of(new EntriesTable(typeManager, tableName, table, ALL_ENTRIES, icebergScanExecutor));
            case ENTRIES -> Optional.of(new EntriesTable(typeManager, tableName, table, ENTRIES, icebergScanExecutor));
            case PROPERTIES -> Optional.of(new PropertiesTable(tableName, table));
            case REFS -> Optional.of(new RefsTable(tableName, table, icebergScanExecutor));
        };
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle table = (IcebergTableHandle) tableHandle;

        if (table.getSnapshotId().isEmpty()) {
            // A table with missing snapshot id produces no splits, so we optimize here by returning
            // TupleDomain.none() as the predicate
            return new ConnectorTableProperties(TupleDomain.none(), Optional.empty(), Optional.empty(), ImmutableList.of());
        }

        Table icebergTable = catalog.loadTable(session, table.getSchemaTableName());

        // Extract identity partition fields that are present in all partition specs, for creating the discrete predicates.
        Set<Integer> partitionSourceIds = identityPartitionColumnsInAllSpecs(icebergTable);

        TupleDomain<IcebergColumnHandle> enforcedPredicate = table.getEnforcedPredicate();

        DiscretePredicates discretePredicates = null;
        if (!partitionSourceIds.isEmpty()) {
            // Extract identity partition columns
            Map<Integer, IcebergColumnHandle> columns = getProjectedColumns(icebergTable.schema(), typeManager, partitionSourceIds).stream()
                    .collect(toImmutableMap(IcebergColumnHandle::getId, identity()));

            Supplier<Map<StructLikeWrapperWithFieldIdToIndex, PartitionSpec>> lazyUniquePartitions = Suppliers.memoize(() -> {
                TableScan tableScan = icebergTable.newScan()
                        .useSnapshot(table.getSnapshotId().getAsLong())
                        .filter(toIcebergExpression(enforcedPredicate))
                        .planWith(icebergPlanningExecutor);

                try (CloseableIterable<FileScanTask> fileScanTasks = tableScan.planFiles()) {
                    Map<StructLikeWrapperWithFieldIdToIndex, PartitionSpec> partitions = new HashMap<>();
                    for (FileScanTask fileScanTask : fileScanTasks) {
                        StructLikeWrapperWithFieldIdToIndex structLikeWrapperWithFieldIdToIndex = createStructLikeWrapper(fileScanTask);
                        partitions.putIfAbsent(structLikeWrapperWithFieldIdToIndex, fileScanTask.spec());
                    }
                    return partitions;
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });

            Iterable<TupleDomain<ColumnHandle>> discreteTupleDomain = Iterables.transform(
                    () -> lazyUniquePartitions.get().entrySet().iterator(),
                    entry -> {
                        // Extract partition values
                        Map<Integer, Optional<String>> partitionColumnValueStrings = getPartitionKeys(entry.getKey().getStructLikeWrapper().get(), entry.getValue());
                        Map<ColumnHandle, NullableValue> partitionValues = partitionSourceIds.stream()
                                .filter(partitionColumnValueStrings::containsKey)
                                .collect(toImmutableMap(
                                        columns::get,
                                        columnId -> {
                                            IcebergColumnHandle column = columns.get(columnId);
                                            Object prestoValue = deserializePartitionValue(
                                                    column.getType(),
                                                    partitionColumnValueStrings.get(columnId).orElse(null),
                                                    column.getName());

                                            return new NullableValue(column.getType(), prestoValue);
                                        }));

                        return TupleDomain.fromFixedValues(partitionValues);
                    });

            discretePredicates = new DiscretePredicates(
                    columns.values().stream()
                            .map(ColumnHandle.class::cast)
                            .collect(toImmutableList()),
                    discreteTupleDomain);
        }

        List<LocalProperty<ColumnHandle>> sortingProperties = ImmutableList.of();
        if (isUnsafeSortingPropertiesEnabled(session)) {
            sortingProperties = getSortingProperties(icebergTable, typeManager);
        }

        return new ConnectorTableProperties(
                // Using the predicate here directly avoids eagerly loading all partition values. Logically, this
                // still keeps predicate and discretePredicates evaluation the same on every row of the table. This
                // can be further optimized by intersecting with partition values at the cost of iterating
                // over all tableScan.planFiles() and caching partition values in table handle.
                enforcedPredicate.transformKeys(ColumnHandle.class::cast),
                table.getTablePartitioning().flatMap(IcebergTablePartitioning::toConnectorTablePartitioning),
                Optional.ofNullable(discretePredicates),
                sortingProperties);
    }

    @Override
    public Optional<ConnectorTableHandle> applyPartitioning(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<ConnectorPartitioningHandle> partitioningHandle, List<ColumnHandle> partitioningColumns)
    {
        IcebergTableHandle icebergTableHandle = checkValidTableHandle(tableHandle);
        if (icebergTableHandle.getSpecId().isEmpty()) {
            return Optional.empty();
        }

        Optional<IcebergTablePartitioning> connectorTablePartitioning = icebergTableHandle.getTablePartitioning();
        if (connectorTablePartitioning.isEmpty()) {
            return Optional.empty();
        }
        IcebergTablePartitioning tablePartitioning = connectorTablePartitioning.get();

        // Check if the table can be partitioned on the requested columns
        if (!new HashSet<>(tablePartitioning.partitioningColumns()).containsAll(partitioningColumns)) {
            return Optional.empty();
        }

        Map<ColumnHandle, Integer> newPartitioningColumnIndex = IntStream.range(0, partitioningColumns.size()).boxed()
                .collect(toImmutableMap(partitioningColumns::get, identity()));
        ImmutableList.Builder<IcebergPartitionFunction> newPartitionFunctions = ImmutableList.builder();
        ImmutableList.Builder<Integer> newPartitionStructFields = ImmutableList.builder();
        for (int functionIndex = 0; functionIndex < tablePartitioning.partitioningHandle().partitionFunctions().size(); functionIndex++) {
            IcebergPartitionFunction function = tablePartitioning.partitioningHandle().partitionFunctions().get(functionIndex);
            int oldColumnIndex = function.dataPath().getFirst();
            Integer newColumnIndex = newPartitioningColumnIndex.get(tablePartitioning.partitioningColumns().get(oldColumnIndex));
            if (newColumnIndex != null) {
                // Change the index of the top level column to the location in the new partitioning columns
                newPartitionFunctions.add(function.withTopLevelColumnIndex(newColumnIndex));
                // Some partition functions may be dropped so update the struct fields used in split partitioning must be updated
                newPartitionStructFields.add(tablePartitioning.partitionStructFields().get(functionIndex));
            }
        }

        IcebergPartitioningHandle newPartitioningHandle = new IcebergPartitioningHandle(false, newPartitionFunctions.build());
        if (partitioningHandle.isPresent() && !partitioningHandle.get().equals(newPartitioningHandle)) {
            // todo if bucketing is a power of two, we can adapt the bucketing
            return Optional.empty();
        }
        if (newPartitioningHandle.partitionFunctions().stream().map(IcebergPartitionFunction::transform).noneMatch(BUCKET::equals)) {
            // The table is only using value-based partitioning, and this can hurt performance if there is a filter
            // on the partitioning columns. This is something we may be able to support with statistics in the future.
            return Optional.empty();
        }

        return Optional.of(icebergTableHandle.withTablePartitioning(Optional.of(new IcebergTablePartitioning(
                true,
                newPartitioningHandle,
                partitioningColumns.stream().map(IcebergColumnHandle.class::cast).collect(toImmutableList()),
                newPartitionStructFields.build()))));
    }

    @Override
    public SchemaTableName getTableName(ConnectorSession session, ConnectorTableHandle table)
    {
        if (table instanceof CorruptedIcebergTableHandle corruptedTableHandle) {
            return corruptedTableHandle.schemaTableName();
        }
        return ((IcebergTableHandle) table).getSchemaTableName();
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        IcebergTableHandle tableHandle = checkValidTableHandle(table);
        // This method does not calculate column metadata for the projected columns
        checkArgument(tableHandle.getProjectedColumns().isEmpty(), "Unexpected projected columns");
        BaseTable icebergTable = catalog.loadTable(session, tableHandle.getSchemaTableName());
        List<ColumnMetadata> columns = getColumnMetadatas(SchemaParser.fromJson(tableHandle.getTableSchemaJson()), typeManager, tableHandle.getFormatVersion());
        return new ConnectorTableMetadata(tableHandle.getSchemaTableName(), columns, getIcebergTableProperties(icebergTable), getTableComment(icebergTable));
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        return catalog.listTables(session, schemaName).stream()
                .map(TableInfo::tableName)
                .toList();
    }

    @Override
    public Map<SchemaTableName, RelationType> getRelationTypes(ConnectorSession session, Optional<String> schemaName)
    {
        ImmutableMap.Builder<SchemaTableName, RelationType> result = ImmutableMap.builder();
        for (TableInfo info : catalog.listTables(session, schemaName)) {
            result.put(info.tableName(), info.extendedRelationType().toRelationType());
        }
        return result.buildKeepingLast();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle table = checkValidTableHandle(tableHandle);
        Map<String, ColumnHandle> columnHandles = new LinkedHashMap<>();
        for (IcebergColumnHandle columnHandle : getTopLevelColumns(SchemaParser.fromJson(table.getTableSchemaJson()), typeManager)) {
            columnHandles.putIfAbsent(columnHandle.getName(), columnHandle);
        }
        if (table.getFormatVersion() >= 3) {
            // It is critical that ROW_ID comes before LAST_UPDATED_SEQUENCE_NUMBER, because the optimize command expects this ordering
            columnHandles.putIfAbsent(ROW_ID.getColumnName(), rowIdColumnHandle());
            columnHandles.putIfAbsent(LAST_UPDATED_SEQUENCE_NUMBER.getColumnName(), lastUpdatedSequenceNumberColumnHandle());
        }
        columnHandles.putIfAbsent(PARTITION.getColumnName(), partitionColumnHandle());
        columnHandles.putIfAbsent(FILE_PATH.getColumnName(), pathColumnHandle());
        columnHandles.putIfAbsent(FILE_MODIFIED_TIME.getColumnName(), fileModifiedTimeColumnHandle());
        return ImmutableMap.copyOf(columnHandles);
    }

    private static boolean containsColumnHandle(List<IcebergColumnHandle> columnHandles, String columnName)
    {
        return columnHandles.stream()
                .anyMatch(columnHandle -> columnHandle.getName().equals(columnName));
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        IcebergTableHandle icebergTableHandle = (IcebergTableHandle) tableHandle;
        IcebergColumnHandle column = (IcebergColumnHandle) columnHandle;

        // Look up write-default from the table schema
        Optional<String> defaultValue = Optional.empty();
        if (!isMetadataColumnId(column.getId())) {
            Schema tableSchema = SchemaParser.fromJson(icebergTableHandle.getTableSchemaJson());
            NestedField field = tableSchema.findField(column.getId());
            if (field != null) {
                defaultValue = formatIcebergDefaultAsSql(field.writeDefault(), field.type());
            }
        }

        return ColumnMetadata.builder()
                .setName(column.getName())
                .setType(column.getType())
                .setNullable(column.isNullable())
                .setComment(column.getComment())
                .setHidden(isMetadataColumnId(column.getId()))
                .setDefaultValue(defaultValue)
                .build();
    }

    @Override
    public void validateScan(ConnectorSession session, ConnectorTableHandle handle)
    {
        IcebergTableHandle table = (IcebergTableHandle) handle;
        if (isQueryPartitionFilterRequiredForTable(session, table) && table.getEnforcedPredicate().isAll() && !table.getForAnalyze().orElseThrow()) {
            Schema schema = SchemaParser.fromJson(table.getTableSchemaJson());
            Optional<PartitionSpec> partitionSpec = table.getSpecId().isPresent() ?
                    Optional.of(PartitionSpecParser.fromJson(schema, table.getPartitionSpecJsons().get(table.getSpecId().getAsInt()))) :
                    Optional.empty();
            if (partitionSpec.isEmpty() || partitionSpec.get().isUnpartitioned()) {
                return;
            }
            Set<Integer> columnsWithPredicates = new HashSet<>();
            table.getConstraintColumns().stream()
                    .map(IcebergColumnHandle::getId)
                    .forEach(columnsWithPredicates::add);
            table.getUnenforcedPredicate().getDomains().ifPresent(domain -> domain.keySet().stream()
                    .map(IcebergColumnHandle::getId)
                    .forEach(columnsWithPredicates::add));
            Set<Integer> partitionColumns = partitionSpec.get().fields().stream()
                    .filter(field -> !field.transform().isVoid())
                    .map(PartitionField::sourceId)
                    .collect(toImmutableSet());
            if (Collections.disjoint(columnsWithPredicates, partitionColumns)) {
                String partitionColumnNames = partitionSpec.get().fields().stream()
                        .filter(field -> !field.transform().isVoid())
                        .map(PartitionField::sourceId)
                        .map(id -> schema.idToName().get(id))
                        .collect(joining(", "));
                throw new TrinoException(
                        QUERY_REJECTED,
                        format("Filter required for %s on at least one of the partition columns: %s", table.getSchemaTableName(), partitionColumnNames));
            }
        }
    }

    private static boolean isQueryPartitionFilterRequiredForTable(ConnectorSession session, IcebergTableHandle table)
    {
        Set<String> requiredSchemas = getQueryPartitionFilterRequiredSchemas(session);
        // If query_partition_filter_required_schemas is empty then we would apply partition filter for all tables.
        return isQueryPartitionFilterRequired(session) &&
                (requiredSchemas.isEmpty() || requiredSchemas.contains(table.getSchemaName()));
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        throw new UnsupportedOperationException("The deprecated listTableColumns is not supported because streamTableColumns is implemented instead");
    }

    @Override
    public Iterator<TableColumnsMetadata> streamTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        List<SchemaTableName> schemaTableNames;
        if (prefix.getTable().isEmpty()) {
            schemaTableNames = catalog.listTables(session, prefix.getSchema()).stream()
                    .map(TableInfo::tableName)
                    .collect(toImmutableList());
        }
        else {
            schemaTableNames = ImmutableList.of(prefix.toSchemaTableName());
        }

        return Lists.partition(schemaTableNames, GET_METADATA_BATCH_SIZE).stream()
                .map(tableBatch -> {
                    ImmutableList.Builder<TableColumnsMetadata> tableMetadatas = ImmutableList.builderWithExpectedSize(tableBatch.size());
                    Set<SchemaTableName> remainingTables = new HashSet<>(tableBatch.size());
                    for (SchemaTableName tableName : tableBatch) {
                        if (redirectTable(session, tableName).isPresent()) {
                            tableMetadatas.add(TableColumnsMetadata.forRedirectedTable(tableName));
                        }
                        else {
                            remainingTables.add(tableName);
                        }
                    }

                    Map<SchemaTableName, List<ColumnMetadata>> loaded = catalog.tryGetColumnMetadata(session, ImmutableList.copyOf(remainingTables));
                    loaded.forEach((tableName, columns) -> {
                        remainingTables.remove(tableName);
                        tableMetadatas.add(TableColumnsMetadata.forTable(tableName, columns));
                    });

                    List<Callable<Optional<TableColumnsMetadata>>> tasks = remainingTables.stream()
                            .map(tableName -> (Callable<Optional<TableColumnsMetadata>>) () -> {
                                try {
                                    Table icebergTable = catalog.loadTable(session, tableName);
                                    List<ColumnMetadata> columns = getColumnMetadatas(icebergTable.schema(), typeManager, formatVersion(icebergTable));
                                    return Optional.of(TableColumnsMetadata.forTable(tableName, columns));
                                }
                                catch (TableNotFoundException e) {
                                    // Table disappeared during listing operation
                                    return Optional.empty();
                                }
                                catch (UnknownTableTypeException e) {
                                    // Skip unsupported table type in case that the table redirects are not enabled
                                    return Optional.empty();
                                }
                                catch (RuntimeException e) {
                                    // Table can be being removed and this may cause all sorts of exceptions. Log, because we're catching broadly.
                                    log.warn(e, "Failed to access metadata of table %s during streaming table columns for %s", tableName, prefix);
                                    return Optional.empty();
                                }
                            })
                            .collect(toImmutableList());

                    try {
                        List<TableColumnsMetadata> taskResults = processWithAdditionalThreads(tasks, metadataFetchingExecutor).stream()
                                .flatMap(Optional::stream) // Flatten the Optionals into a stream
                                .collect(toImmutableList());

                        tableMetadatas.addAll(taskResults);
                    }
                    catch (ExecutionException e) {
                        throw new RuntimeException(e.getCause());
                    }

                    return tableMetadatas.build();
                })
                .flatMap(List::stream)
                .iterator();
    }

    @Override
    public Iterator<RelationColumnsMetadata> streamRelationColumns(ConnectorSession session, Optional<String> schemaName, UnaryOperator<Set<SchemaTableName>> relationFilter)
    {
        return catalog.streamRelationColumns(session, schemaName, relationFilter, tableName -> redirectTable(session, tableName).isPresent())
                .orElseGet(() -> {
                    // Catalog does not support streamRelationColumns
                    return ConnectorMetadata.super.streamRelationColumns(session, schemaName, relationFilter);
                });
    }

    @Override
    public Iterator<RelationCommentMetadata> streamRelationComments(ConnectorSession session, Optional<String> schemaName, UnaryOperator<Set<SchemaTableName>> relationFilter)
    {
        return catalog.streamRelationComments(session, schemaName, relationFilter, tableName -> redirectTable(session, tableName).isPresent())
                .orElseGet(() -> {
                    // Catalog does not support streamRelationComments
                    return ConnectorMetadata.super.streamRelationComments(session, schemaName, relationFilter);
                });
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties, TrinoPrincipal owner)
    {
        Optional.ofNullable(properties.get(LOCATION_PROPERTY)).ifPresent(location -> locationAccessControl.checkCanUseLocation(session.getIdentity(), (String) location));
        catalog.createNamespace(session, schemaName, properties, owner);
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName, boolean cascade)
    {
        if (cascade) {
            List<String> nestedNamespaces = getChildNamespaces(session, schemaName);
            if (!nestedNamespaces.isEmpty()) {
                throw new TrinoException(
                        ICEBERG_CATALOG_ERROR,
                        format("Cannot drop non-empty schema: %s, contains %s nested schema(s)", schemaName, Joiner.on(", ").join(nestedNamespaces)));
            }

            for (SchemaTableName materializedView : listMaterializedViews(session, Optional.of(schemaName))) {
                dropMaterializedView(session, materializedView);
            }
            for (SchemaTableName viewName : listViews(session, Optional.of(schemaName))) {
                dropView(session, viewName);
            }
            for (SchemaTableName tableName : listTables(session, Optional.of(schemaName))) {
                ConnectorTableHandle tableHandle = getTableHandle(session, tableName, Optional.empty(), Optional.empty());
                if (tableHandle != null) {
                    // getTableHandle method returns null if the table is dropped concurrently
                    dropTable(session, tableHandle);
                }
            }
        }
        catalog.dropNamespace(session, schemaName);
    }

    @Override
    public void renameSchema(ConnectorSession session, String source, String target)
    {
        catalog.renameNamespace(session, source, target);
    }

    @Override
    public void setSchemaAuthorization(ConnectorSession session, String schemaName, TrinoPrincipal principal)
    {
        catalog.setNamespacePrincipal(session, schemaName, principal);
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, SaveMode saveMode)
    {
        Optional<ConnectorTableLayout> layout = getNewTableLayout(session, tableMetadata);
        finishCreateTable(session, beginCreateTable(session, tableMetadata, layout, NO_RETRIES, saveMode == SaveMode.REPLACE), ImmutableList.of(), ImmutableList.of());
    }

    @Override
    public void setTableComment(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<String> comment)
    {
        IcebergTableHandle handle = checkValidTableHandle(tableHandle);
        catalog.updateTableComment(session, handle.getSchemaTableName(), comment);
    }

    @Override
    public void setViewComment(ConnectorSession session, SchemaTableName viewName, Optional<String> comment)
    {
        catalog.updateViewComment(session, viewName, comment);
    }

    @Override
    public void setViewColumnComment(ConnectorSession session, SchemaTableName viewName, String columnName, Optional<String> comment)
    {
        catalog.updateViewColumnComment(session, viewName, columnName, comment);
    }

    @Override
    public void setMaterializedViewColumnComment(ConnectorSession session, SchemaTableName viewName, String columnName, Optional<String> comment)
    {
        catalog.updateMaterializedViewColumnComment(session, viewName, columnName, comment);
    }

    @Override
    public Optional<ConnectorTableLayout> getNewTableLayout(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        Schema schema = schemaFromMetadata(tableMetadata.getColumns());
        PartitionSpec partitionSpec = parsePartitionFields(schema, getPartitioning(tableMetadata.getProperties()));
        return getWriteLayout(schema, partitionSpec, false);
    }

    @Override
    public Optional<io.trino.spi.type.Type> getSupportedType(ConnectorSession session, Map<String, Object> tableProperties, io.trino.spi.type.Type type)
    {
        int formatVersion = getFormatVersion(tableProperties);
        io.trino.spi.type.Type newType = coerceType(formatVersion, type);
        if (type.equals(newType)) {
            return Optional.empty();
        }
        return Optional.of(newType);
    }

    private io.trino.spi.type.Type coerceType(int formatVersion, io.trino.spi.type.Type type)
    {
        if (type == TINYINT || type == SMALLINT) {
            return INTEGER;
        }
        if (type instanceof TimestampWithTimeZoneType timestampWithTimeZoneType) {
            if (formatVersion >= TIMESTAMP_NANOS_SUPPORTED_MIN_VERSION && timestampWithTimeZoneType.getPrecision() > TIMESTAMP_TZ_MICROS.getPrecision()) {
                return TIMESTAMP_TZ_NANOS;
            }
            return TIMESTAMP_TZ_MICROS;
        }
        if (type instanceof TimestampType timestampType) {
            if (formatVersion >= TIMESTAMP_NANOS_SUPPORTED_MIN_VERSION && timestampType.getPrecision() > TIMESTAMP_MICROS.getPrecision()) {
                return TIMESTAMP_NANOS;
            }
            return TIMESTAMP_MICROS;
        }
        if (type instanceof TimeType) {
            return TIME_MICROS;
        }
        if (type instanceof CharType) {
            return VARCHAR;
        }
        if (type instanceof ArrayType arrayType) {
            return new ArrayType(coerceType(formatVersion, arrayType.getElementType()));
        }
        if (type instanceof MapType mapType) {
            return new MapType(coerceType(formatVersion, mapType.getKeyType()), coerceType(formatVersion, mapType.getValueType()), typeManager.getTypeOperators());
        }
        if (type instanceof RowType rowType) {
            return RowType.from(rowType.getFields().stream()
                    .map(field -> new RowType.Field(field.getName(), coerceType(formatVersion, field.getType())))
                    .collect(toImmutableList()));
        }
        return type;
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorTableLayout> layout, RetryMode retryMode, boolean replace)
    {
        verify(transaction == null, "transaction already set");
        String schemaName = tableMetadata.getTable().getSchemaName();
        if (!schemaExists(session, schemaName)) {
            throw new SchemaNotFoundException(schemaName);
        }

        int formatVersion = getFormatVersion(tableMetadata.getProperties());
        tableMetadata.getColumns().forEach(column -> checkDefaultValueCompatibility(formatVersion, column));

        String tableLocation = null;
        if (replace) {
            ConnectorTableHandle tableHandle = getTableHandle(session, tableMetadata.getTableSchema().getTable(), Optional.empty(), Optional.empty());
            if (tableHandle != null) {
                checkValidTableHandle(tableHandle);
                IcebergTableHandle table = (IcebergTableHandle) tableHandle;
                verifyTableVersionForUpdate(table);
                Table icebergTable = catalog.loadTable(session, table.getSchemaTableName());
                Optional<String> providedTableLocation = getTableLocation(tableMetadata.getProperties());
                if (providedTableLocation.isPresent() && !stripTrailingSlash(providedTableLocation.get()).equals(icebergTable.location())) {
                    throw new TrinoException(INVALID_TABLE_PROPERTY, format("The provided location '%s' does not match the existing table location '%s'", providedTableLocation.get(), icebergTable.location()));
                }
                validateNotModifyingOldSnapshot(table, icebergTable);
                tableLocation = icebergTable.location();
                List<PartitionField> existingPartitionFields = getAllPartitionFields(icebergTable);
                transaction = newCreateTableTransaction(catalog, tableMetadata, session, replace, tableLocation, allowedExtraProperties, existingPartitionFields);
            }
        }

        if (tableLocation == null) {
            tableLocation = getTableLocation(tableMetadata.getProperties())
                    .map(location -> {
                        locationAccessControl.checkCanUseLocation(session.getIdentity(), location);
                        return location;
                    })
                    .orElseGet(() -> catalog.defaultTableLocation(session, tableMetadata.getTable()));
        }

        if (transaction == null) {
            transaction = newCreateTableTransaction(catalog, tableMetadata, session, replace, tableLocation, allowedExtraProperties, ImmutableList.of());
        }
        Location location = Location.of(transaction.table().location());
        try {
            // S3 Tables internally assigns a unique location for each table
            if (!isS3Tables(location.toString())) {
                TrinoFileSystem fileSystem = fileSystemFactory.create(session.getIdentity(), transaction.table().io().properties());
                if (!replace && fileSystem.listFiles(location).hasNext()) {
                    throw new TrinoException(ICEBERG_FILESYSTEM_ERROR, format("" +
                            "Cannot create a table on a non-empty location: %s, set 'iceberg.unique-table-location=true' in your Iceberg catalog properties " +
                            "to use unique table locations for every table.", location));
                }
            }
            return newWritableTableHandle(tableMetadata.getTable(), transaction.table(), SchemaParser.toJson(transaction.table().schema()), Optional.empty(), ImmutableList.of());
        }
        catch (IOException | UncheckedIOException e) {
            throw new TrinoException(ICEBERG_FILESYSTEM_ERROR, "Failed checking new table's location: " + location, e);
        }
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        IcebergWritableTableHandle icebergTableHandle = (IcebergWritableTableHandle) tableHandle;
        try {
            if (fragments.isEmpty()) {
                // Commit the transaction if the table is being created without data
                AppendFiles appendFiles = transaction.newFastAppend();
                commitUpdateAndTransaction(appendFiles, session, transaction, "create table");
                transaction = null;
                return Optional.empty();
            }

            return finishInsert(session, icebergTableHandle, ImmutableList.of(), fragments, computedStatistics);
        }
        catch (AlreadyExistsException e) {
            // May happen when table has been already created concurrently.
            throw new TrinoException(TABLE_ALREADY_EXISTS, format("Table %s already exists", icebergTableHandle.name()), e);
        }
    }

    @Override
    public Optional<ConnectorTableLayout> getInsertLayout(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle table = (IcebergTableHandle) tableHandle;
        Schema schema = SchemaParser.fromJson(table.getTableSchemaJson());
        int specId = table.getSpecId().orElseThrow(() -> new VerifyException("Partition spec missing in the table handle"));
        PartitionSpec partitionSpec = PartitionSpecParser.fromJson(schema, table.getPartitionSpecJsons().get(specId));
        return getWriteLayout(schema, partitionSpec, false);
    }

    private Optional<ConnectorTableLayout> getWriteLayout(Schema tableSchema, PartitionSpec partitionSpec, boolean forceRepartitioning)
    {
        if (partitionSpec.isUnpartitioned()) {
            return Optional.empty();
        }

        StructType schemaAsStruct = tableSchema.asStruct();
        Map<Integer, NestedField> indexById = TypeUtil.indexById(schemaAsStruct);
        Map<Integer, Integer> indexParents = indexParents(schemaAsStruct);
        Map<Integer, List<Integer>> indexPaths = indexById.entrySet().stream()
                .collect(toImmutableMap(Entry::getKey, entry -> ImmutableList.copyOf(buildPath(indexParents, entry.getKey()))));

        List<IcebergColumnHandle> partitioningColumns = partitionSpec.fields().stream()
                .sorted(Comparator.comparingInt(PartitionField::sourceId))
                .map(field -> {
                    boolean isBaseColumn = !indexParents.containsKey(field.sourceId());
                    int sourceId;
                    if (isBaseColumn) {
                        sourceId = field.sourceId();
                    }
                    else {
                        sourceId = getRootFieldId(indexParents, field.sourceId());
                    }
                    Type sourceType = tableSchema.findType(sourceId);
                    // The source column, must be a primitive type and cannot be contained in a map or list, but may be nested in a struct.
                    // https://iceberg.apache.org/spec/#partitioning
                    if (sourceType.isMapType()) {
                        throw new TrinoException(NOT_SUPPORTED, "Partitioning field [" + field.name() + "] cannot be contained in a map");
                    }
                    if (sourceType.isListType()) {
                        throw new TrinoException(NOT_SUPPORTED, "Partitioning field [" + field.name() + "] cannot be contained in a array");
                    }
                    verify(indexById.containsKey(sourceId), "Cannot find source column for partition field %s", field);
                    return createColumnHandle(typeManager, sourceId, indexById, indexPaths);
                })
                .distinct()
                .collect(toImmutableList());
        List<String> partitioningColumnNames = partitioningColumns.stream()
                .map(column -> column.getName().toLowerCase(ENGLISH))
                .collect(toImmutableList());

        if (!forceRepartitioning && partitionSpec.fields().stream().allMatch(field -> field.transform().isIdentity())) {
            // Do not set partitioningHandle, to let engine determine whether to repartition data or not, on stat-based basis.
            return Optional.of(new ConnectorTableLayout(partitioningColumnNames));
        }
        IcebergPartitioningHandle partitioningHandle = IcebergPartitioningHandle.create(partitionSpec, typeManager, List.of());
        return Optional.of(new ConnectorTableLayout(partitioningHandle, partitioningColumnNames, true));
    }

    private static int getRootFieldId(Map<Integer, Integer> indexParents, int fieldId)
    {
        int rootFieldId = fieldId;
        while (indexParents.containsKey(rootFieldId)) {
            rootFieldId = indexParents.get(rootFieldId);
        }
        return rootFieldId;
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> columns, RetryMode retryMode)
    {
        IcebergTableHandle table = (IcebergTableHandle) tableHandle;
        BaseTable icebergTable = catalog.loadTable(session, table.getSchemaTableName());
        Optional<String> branch = table.getBranch();

        branch.ifPresentOrElse(
                name -> checkBranch(icebergTable, name),
                () -> validateNotModifyingOldSnapshot(table, icebergTable));

        validateTableForTrino(icebergTable, getCurrentSnapshotId(icebergTable));

        beginTransaction(icebergTable);

        return newWritableTableHandle(table.getSchemaTableName(), icebergTable, table.getTableSchemaJson(), branch, ImmutableList.of());
    }

    private List<String> getChildNamespaces(ConnectorSession session, String parentNamespace)
    {
        Optional<String> namespaceSeparator = catalog.getNamespaceSeparator();

        if (namespaceSeparator.isEmpty()) {
            return ImmutableList.of();
        }

        return catalog.listNamespaces(session).stream()
                .filter(namespace -> namespace.startsWith(parentNamespace + namespaceSeparator.get()))
                .collect(toImmutableList());
    }

    private IcebergWritableTableHandle newWritableTableHandle(
            SchemaTableName name,
            Table table,
            String schemaAsJson,
            Optional<String> branch,
            List<PositionDeleteFiles> previousDeleteFiles)
    {
        Schema schema = SchemaParser.fromJson(schemaAsJson);
        SortFieldInfo sortInfo = getSupportedSortFields(schema, table.sortOrder());
        tableCredentialsCache.put(name, IcebergTableCredentials.forFileIO(table.io()));
        return new IcebergWritableTableHandle(
                name,
                formatVersion(table),
                schemaAsJson,
                transformValues(table.specs(), PartitionSpecParser::toJson),
                table.spec().specId(),
                sortInfo.supportedSortFields(),
                sortInfo.sortOrderId(),
                getPartitionColumns(table, typeManager),
                previousDeleteFiles,
                table.location(),
                getFileFormat(table),
                table.properties(),
                rowLevelOperationMode(table),
                branch);
    }

    private static SortFieldInfo getSupportedSortFields(Schema schema, SortOrder sortOrder)
    {
        if (!sortOrder.isSorted()) {
            return new SortFieldInfo(SortOrder.unsorted().orderId(), ImmutableList.of());
        }
        Set<Integer> baseColumnFieldIds = schema.columns().stream()
                .map(Types.NestedField::fieldId)
                .collect(toImmutableSet());

        ImmutableList.Builder<TrinoSortField> sortFields = ImmutableList.builder();
        for (SortField sortField : sortOrder.fields()) {
            if (!sortField.transform().isIdentity()) {
                continue;
            }
            if (!baseColumnFieldIds.contains(sortField.sourceId())) {
                continue;
            }

            sortFields.add(TrinoSortField.fromIceberg(sortField));
        }

        List<TrinoSortField> supportedSortFields = sortFields.build();
        // Trino supports sorting only by identity transforms, so the supported sort fields in Trino may be a subset of the iceberg table sort order.
        // The writer can populate sort_order_id in the iceberg metadata only when the iceberg table sort order is fully supported by Trino.
        if (supportedSortFields.size() == sortOrder.fields().size()) {
            return new SortFieldInfo(sortOrder.orderId(), supportedSortFields);
        }
        return new SortFieldInfo(SortOrder.unsorted().orderId(), supportedSortFields);
    }

    private static List<LocalProperty<ColumnHandle>> getSortingProperties(Table icebergTable, TypeManager typeManager)
    {
        Schema schema = icebergTable.schema();
        SortOrder sortOrder = icebergTable.sortOrder();

        SortFieldInfo sortInfo = getSupportedSortFields(schema, sortOrder);
        if (sortInfo.supportedSortFields().isEmpty()) {
            return ImmutableList.of();
        }

        Set<Integer> sortFieldIds = sortInfo.supportedSortFields().stream()
                .map(TrinoSortField::sourceColumnId)
                .collect(toImmutableSet());

        Map<Integer, IcebergColumnHandle> columnHandlesById = getProjectedColumns(schema, typeManager, sortFieldIds).stream()
                .collect(toImmutableMap(IcebergColumnHandle::getId, identity()));

        ImmutableList.Builder<LocalProperty<ColumnHandle>> sortingProperties = ImmutableList.builder();
        for (TrinoSortField sortField : sortInfo.supportedSortFields()) {
            IcebergColumnHandle columnHandle = columnHandlesById.get(sortField.sourceColumnId());
            sortingProperties.add(new SortingProperty<>(columnHandle, sortField.sortOrder()));
        }

        return sortingProperties.build();
    }

    private record SortFieldInfo(int sortOrderId, List<TrinoSortField> supportedSortFields) {}

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(
            ConnectorSession session,
            ConnectorInsertTableHandle insertHandle,
            List<ConnectorTableHandle> sourceTableHandles,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics)
    {
        List<CommitTaskData> commitTasks = fragments.stream()
                .map(Slice::getInput)
                .map(commitTaskCodec::fromJson)
                .collect(toImmutableList());

        if (commitTasks.isEmpty()) {
            transaction = null;
            return Optional.empty();
        }

        IcebergWritableTableHandle table = (IcebergWritableTableHandle) insertHandle;
        Table icebergTable = transaction.table();
        Schema schema = icebergTable.schema();
        Type[] partitionColumnTypes = icebergTable.spec().fields().stream()
                .map(field -> field.transform().getResultType(
                        schema.findType(field.sourceId())))
                .toArray(Type[]::new);

        AppendFiles appendFiles = isMergeManifestsOnWrite(session) ? transaction.newAppend() : transaction.newFastAppend();
        Map<Integer, SortOrder> sortOrders = icebergTable.sortOrders();
        for (CommitTaskData task : commitTasks) {
            DataFiles.Builder builder = DataFiles.builder(icebergTable.spec())
                    .withPath(task.path())
                    .withFileSizeInBytes(task.fileSizeInBytes())
                    .withFormat(table.fileFormat().toIceberg())
                    .withMetrics(task.metrics().metrics())
                    .withSortOrder(sortOrders.get(task.sortOrderId()));
            task.fileSplitOffsets().ifPresent(builder::withSplitOffsets);

            if (!icebergTable.spec().fields().isEmpty()) {
                String partitionDataJson = task.partitionDataJson()
                        .orElseThrow(() -> new VerifyException("No partition data for partitioned table"));
                builder.withPartition(PartitionData.fromJson(partitionDataJson, partitionColumnTypes));
            }

            appendFiles.appendFile(builder.build());
            table.branch().ifPresent(appendFiles::toBranch);
        }

        appendFiles.scanManifestsWith(icebergScanExecutor);
        commitUpdate(appendFiles, session, "insert");

        if (isS3Tables(icebergTable.location())) {
            log.debug("S3 Tables do not support statistics: %s", table.name());
        }
        else if (!computedStatistics.isEmpty()) {
            long newSnapshotId = table.branch().isEmpty() ? icebergTable.currentSnapshot().snapshotId() : icebergTable.refs().get(table.branch().get()).snapshotId();

            CollectedStatistics collectedStatistics = processComputedTableStatistics(icebergTable, computedStatistics);
            StatisticsFile statisticsFile = tableStatisticsWriter.writeStatisticsFile(
                    session,
                    icebergTable,
                    newSnapshotId,
                    INCREMENTAL_UPDATE,
                    collectedStatistics);
            transaction.updateStatistics()
                    .setStatistics(statisticsFile)
                    .commit();
            partitionStatisticsWriter.writePartitionStats(session, icebergTable, newSnapshotId).ifPresent(partitionStatisticsFile -> {
                transaction.updatePartitionStatistics()
                        .setPartitionStatistics(partitionStatisticsFile)
                        .commit();
            });
        }
        commitTransaction(transaction, "insert");
        transaction = null;

        Map<String, String> summary = icebergTable.currentSnapshot().summary();
        if (summary == null) {
            return Optional.empty();
        }
        return Optional.of(new IcebergCommitMetadata(summary));
    }

    @Override
    public Optional<ConnectorTableExecuteHandle> getTableHandleForExecute(
            ConnectorSession session,
            ConnectorAccessControl accessControl,
            ConnectorTableHandle connectorTableHandle,
            String procedureName,
            Map<String, Object> executeProperties,
            RetryMode retryMode)
    {
        IcebergTableHandle tableHandle = (IcebergTableHandle) connectorTableHandle;
        checkArgument(tableHandle.getTableType() == DATA, "Cannot execute table procedure %s on non-DATA table: %s", procedureName, tableHandle.getTableType());
        Table icebergTable = catalog.loadTable(session, tableHandle.getSchemaTableName());
        if (tableHandle.getSnapshotId().isPresent() && (tableHandle.getSnapshotId().getAsLong() != icebergTable.currentSnapshot().snapshotId())) {
            throw new TrinoException(NOT_SUPPORTED, "Cannot execute table procedure %s on old snapshot %s".formatted(procedureName, tableHandle.getSnapshotId().getAsLong()));
        }

        IcebergTableProcedureId procedureId;
        try {
            procedureId = IcebergTableProcedureId.valueOf(procedureName);
        }
        catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Unknown procedure '" + procedureName + "'");
        }

        return switch (procedureId) {
            case OPTIMIZE -> getTableHandleForOptimize(tableHandle, icebergTable, executeProperties);
            case OPTIMIZE_MANIFESTS -> getTableHandleForOptimizeManifests(session, tableHandle);
            case OPTIMIZE_POSITION_DELETES -> getTableHandleForOptimizePositionDeletes(session, tableHandle);
            case REMOVE_DANGLING_DELETE_FILES -> getTableHandleForRemoveDanglingDeleteFiles(session, tableHandle);
            case DROP_EXTENDED_STATS -> getTableHandleForDropExtendedStats(session, tableHandle);
            case ROLLBACK_TO_SNAPSHOT -> getTableHandleForRollbackToSnapshot(session, tableHandle, executeProperties);
            case EXPIRE_SNAPSHOTS -> getTableHandleForExpireSnapshots(session, tableHandle, executeProperties);
            case REMOVE_ORPHAN_FILES -> getTableHandleForRemoveOrphanFiles(session, tableHandle, executeProperties);
            case ADD_FILES -> getTableHandleForAddFiles(session, accessControl, tableHandle, executeProperties);
            case ADD_FILES_FROM_TABLE -> getTableHandleForAddFilesFromTable(session, accessControl, tableHandle, executeProperties);
            case GENERATE_EMBEDDINGS -> getTableHandleForGenerateEmbeddings(session, accessControl, tableHandle, executeProperties, retryMode);
        };
    }

    private Optional<ConnectorTableExecuteHandle> getTableHandleForOptimize(
            IcebergTableHandle tableHandle,
            Table icebergTable,
            Map<String, Object> executeProperties)
    {
        DataSize maxScannedFileSize = (DataSize) executeProperties.get("file_size_threshold");
        SortFieldInfo sortInfo = getSupportedSortFields(icebergTable.schema(), icebergTable.sortOrder());
        int specId = tableHandle.getSpecId().orElseThrow(() -> new VerifyException("Partition spec missing in the table handle"));

        String tableSchemaJson = tableHandle.getTableSchemaJson();

        if (tableHandle.getFormatVersion() >= 3) {
            Schema tableSchema = SchemaParser.fromJson(tableSchemaJson);
            // The order of ROW_ID and LAST_UPDATED_SEQUENCE_NUMBER must match the order in getColumnHandles method
            tableSchema = new Schema(
                    ImmutableList.<NestedField>builder()
                            .addAll(tableSchema.columns())
                            .add(MetadataColumns.ROW_ID)
                            .add(MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER)
                            .build());
            tableSchemaJson = SchemaParser.toJson(tableSchema);
        }
        return Optional.of(new IcebergTableExecuteHandle(
                tableHandle.getSchemaTableName(),
                OPTIMIZE,
                new IcebergOptimizeHandle(
                        tableHandle.getSnapshotId(),
                        tableSchemaJson,
                        tableHandle.getPartitionSpecJsons().get(specId),
                        getPartitionColumns(icebergTable, typeManager),
                        sortInfo.supportedSortFields(),
                        sortInfo.sortOrderId(),
                        getFileFormat(tableHandle.getStorageProperties()),
                        tableHandle.getStorageProperties(),
                        maxScannedFileSize),
                tableHandle.getTableLocation(),
                tableHandle.getFormatVersion()));
    }

    private Optional<ConnectorTableExecuteHandle> getTableHandleForOptimizeManifests(ConnectorSession session, IcebergTableHandle tableHandle)
    {
        Table icebergTable = catalog.loadTable(session, tableHandle.getSchemaTableName());

        return Optional.of(new IcebergTableExecuteHandle(
                tableHandle.getSchemaTableName(),
                OPTIMIZE_MANIFESTS,
                new IcebergOptimizeManifestsHandle(),
                icebergTable.location(),
                tableHandle.getFormatVersion()));
    }

    private Optional<ConnectorTableExecuteHandle> getTableHandleForOptimizePositionDeletes(ConnectorSession session, IcebergTableHandle tableHandle)
    {
        Table icebergTable = catalog.loadTable(session, tableHandle.getSchemaTableName());

        return Optional.of(new IcebergTableExecuteHandle(
                tableHandle.getSchemaTableName(),
                OPTIMIZE_POSITION_DELETES,
                new IcebergOptimizePositionDeletesHandle(),
                icebergTable.location(),
                tableHandle.getFormatVersion()));
    }

    private Optional<ConnectorTableExecuteHandle> getTableHandleForRemoveDanglingDeleteFiles(ConnectorSession session, IcebergTableHandle tableHandle)
    {
        Table icebergTable = catalog.loadTable(session, tableHandle.getSchemaTableName());

        return Optional.of(new IcebergTableExecuteHandle(
                tableHandle.getSchemaTableName(),
                REMOVE_DANGLING_DELETE_FILES,
                new IcebergRemoveDanglingDeleteFilesHandle(),
                icebergTable.location(),
                tableHandle.getFormatVersion()));
    }

    private Optional<ConnectorTableExecuteHandle> getTableHandleForDropExtendedStats(ConnectorSession session, IcebergTableHandle tableHandle)
    {
        Table icebergTable = catalog.loadTable(session, tableHandle.getSchemaTableName());

        return Optional.of(new IcebergTableExecuteHandle(
                tableHandle.getSchemaTableName(),
                DROP_EXTENDED_STATS,
                new IcebergDropExtendedStatsHandle(),
                icebergTable.location(),
                tableHandle.getFormatVersion()));
    }

    private Optional<ConnectorTableExecuteHandle> getTableHandleForExpireSnapshots(ConnectorSession session, IcebergTableHandle tableHandle, Map<String, Object> executeProperties)
    {
        Duration retentionThreshold = (Duration) executeProperties.get(RETENTION_THRESHOLD);
        Table icebergTable = catalog.loadTable(session, tableHandle.getSchemaTableName());
        int retainLast = requireNonNullElse(
                (Integer) executeProperties.get("retain_last"),
                propertyAsInt(icebergTable.properties(), MIN_SNAPSHOTS_TO_KEEP, MIN_SNAPSHOTS_TO_KEEP_DEFAULT));
        boolean cleanExpiredMetadata = (boolean) executeProperties.get("clean_expired_metadata");

        return Optional.of(new IcebergTableExecuteHandle(
                tableHandle.getSchemaTableName(),
                EXPIRE_SNAPSHOTS,
                new IcebergExpireSnapshotsHandle(retentionThreshold, retainLast, cleanExpiredMetadata),
                icebergTable.location(),
                tableHandle.getFormatVersion()));
    }

    private Optional<ConnectorTableExecuteHandle> getTableHandleForRemoveOrphanFiles(ConnectorSession session, IcebergTableHandle tableHandle, Map<String, Object> executeProperties)
    {
        Duration retentionThreshold = (Duration) executeProperties.get(RETENTION_THRESHOLD);
        Table icebergTable = catalog.loadTable(session, tableHandle.getSchemaTableName());

        return Optional.of(new IcebergTableExecuteHandle(
                tableHandle.getSchemaTableName(),
                REMOVE_ORPHAN_FILES,
                new IcebergRemoveOrphanFilesHandle(retentionThreshold),
                icebergTable.location(),
                tableHandle.getFormatVersion()));
    }

    private Optional<ConnectorTableExecuteHandle> getTableHandleForAddFiles(ConnectorSession session, ConnectorAccessControl accessControl, IcebergTableHandle tableHandle, Map<String, Object> executeProperties)
    {
        if (!addFilesProcedureEnabled) {
            throw new TrinoException(PERMISSION_DENIED, "add_files procedure is disabled");
        }

        accessControl.checkCanInsertIntoTable(null, tableHandle.getSchemaTableName());

        String location = (String) requireProcedureArgument(executeProperties, "location");
        HiveStorageFormat format = (HiveStorageFormat) requireProcedureArgument(executeProperties, "format");
        RecursiveDirectory recursiveDirectory = (RecursiveDirectory) executeProperties.getOrDefault("recursive_directory", "fail");

        locationAccessControl.checkCanUseLocation(session.getIdentity(), location);

        Table icebergTable = catalog.loadTable(session, tableHandle.getSchemaTableName());

        if (supportsRowLineage(tableHandle.getFormatVersion()) && icebergTable.schema().findField("_row_id") != null) {
            // https://github.com/apache/iceberg/issues/13232
            throw new TrinoException(NOT_SUPPORTED, "Cannot execute add_files procedure when the table contains _row_id column");
        }

        return Optional.of(new IcebergTableExecuteHandle(
                tableHandle.getSchemaTableName(),
                ADD_FILES,
                new IcebergAddFilesHandle(location, format, recursiveDirectory),
                icebergTable.location(),
                tableHandle.getFormatVersion()));
    }

    private Optional<ConnectorTableExecuteHandle> getTableHandleForAddFilesFromTable(ConnectorSession session, ConnectorAccessControl accessControl, IcebergTableHandle tableHandle, Map<String, Object> executeProperties)
    {
        accessControl.checkCanInsertIntoTable(null, tableHandle.getSchemaTableName());

        String schemaName = (String) requireProcedureArgument(executeProperties, "schema_name");
        String tableName = (String) requireProcedureArgument(executeProperties, "table_name");
        @SuppressWarnings("unchecked")
        Map<String, String> partitionFilter = (Map<String, String>) executeProperties.get("partition_filter");
        RecursiveDirectory recursiveDirectory = (RecursiveDirectory) executeProperties.getOrDefault("recursive_directory", "fail");

        Table icebergTable = catalog.loadTable(session, tableHandle.getSchemaTableName());

        HiveMetastore metastore = metastoreFactory.orElseThrow(() -> new TrinoException(NOT_SUPPORTED, "This catalog does not support add_files_from_table procedure"))
                .createMetastore(Optional.of(session.getIdentity()));
        SchemaTableName sourceName = new SchemaTableName(schemaName, tableName);
        io.trino.metastore.Table sourceTable = metastore.getTable(schemaName, tableName).orElseThrow(() -> new TableNotFoundException(sourceName));
        accessControl.checkCanSelectFromColumns(null, sourceName, Stream.concat(sourceTable.getDataColumns().stream(), sourceTable.getPartitionColumns().stream())
                .map(Column::getName)
                .collect(toImmutableSet()));

        if (supportsRowLineage(tableHandle.getFormatVersion()) && icebergTable.schema().findField("_row_id") != null) {
            // https://github.com/apache/iceberg/issues/13232
            throw new TrinoException(NOT_SUPPORTED, "Cannot execute add_files_from_table procedure when the table contains _row_id column");
        }

        checkProcedureArgument(
                icebergTable.schema().columns().size() >= sourceTable.getDataColumns().size(),
                "Target table should have at least %d columns but got %d", sourceTable.getDataColumns().size(), icebergTable.schema().columns().size());
        checkProcedureArgument(
                icebergTable.spec().fields().size() == sourceTable.getPartitionColumns().size(),
                "Numbers of partition columns should be equivalent. target: %d, source: %d", icebergTable.spec().fields().size(), sourceTable.getPartitionColumns().size());

        // TODO Add files from all partitions when partition filter is not provided
        checkProcedureArgument(
                sourceTable.getPartitionColumns().isEmpty() || partitionFilter != null,
                "partition_filter argument must be provided for partitioned tables");

        String transactionalProperty = sourceTable.getParameters().get(TRANSACTIONAL);
        if (parseBoolean(transactionalProperty)) {
            throw new TrinoException(NOT_SUPPORTED, "Adding files from transactional tables is unsupported");
        }
        if (!"MANAGED_TABLE".equalsIgnoreCase(sourceTable.getTableType()) && !"EXTERNAL_TABLE".equalsIgnoreCase(sourceTable.getTableType())) {
            throw new TrinoException(NOT_SUPPORTED, "The procedure doesn't support adding files from %s table type".formatted(sourceTable.getTableType()));
        }
        if (isSomeKindOfAView(sourceTable) || isIcebergTable(sourceTable) || isDeltaLakeTable(sourceTable) || isHudiTable(sourceTable)) {
            throw new TrinoException(NOT_SUPPORTED, "Adding files from non-Hive tables is unsupported");
        }
        if (sourceTable.getPartitionColumns().isEmpty() && partitionFilter != null && !partitionFilter.isEmpty()) {
            throw new TrinoException(NOT_SUPPORTED, "Partition filter is not supported for non-partitioned tables");
        }

        Set<String> missingDataColumns = new HashSet<>();
        Stream.of(sourceTable.getDataColumns(), sourceTable.getPartitionColumns())
                .flatMap(List::stream)
                .forEach(sourceColumn -> {
                    Types.NestedField targetColumn = icebergTable.schema().caseInsensitiveFindField(sourceColumn.getName());
                    if (targetColumn == null) {
                        if (sourceTable.getPartitionColumns().contains(sourceColumn)) {
                            throw new TrinoException(COLUMN_NOT_FOUND, "Partition column '%s' does not exist".formatted(sourceColumn.getName()));
                        }
                        missingDataColumns.add(sourceColumn.getName());
                        return;
                    }
                    ColumnIdentity columnIdentity = createColumnIdentity(targetColumn);
                    org.apache.iceberg.types.Type sourceColumnType = toIcebergType(typeManager.getType(getTypeSignature(sourceColumn.getType(), DEFAULT_PRECISION)), columnIdentity);
                    if (!targetColumn.type().equals(sourceColumnType)) {
                        throw new TrinoException(TYPE_MISMATCH, "Target '%s' column is '%s' type, but got source '%s' type".formatted(targetColumn.name(), targetColumn.type(), sourceColumnType));
                    }
                });
        if (missingDataColumns.size() == sourceTable.getDataColumns().size()) {
            throw new TrinoException(COLUMN_NOT_FOUND, "All columns in the source table do not exist in the target table");
        }

        return Optional.of(new IcebergTableExecuteHandle(
                tableHandle.getSchemaTableName(),
                ADD_FILES_FROM_TABLE,
                new IcebergAddFilesFromTableHandle(sourceTable, partitionFilter, recursiveDirectory),
                icebergTable.location(),
                tableHandle.getFormatVersion()));
    }

    private Optional<ConnectorTableExecuteHandle> getTableHandleForRollbackToSnapshot(ConnectorSession session, IcebergTableHandle tableHandle, Map<String, Object> executeProperties)
    {
        long snapshotId = (long) executeProperties.get("snapshot_id");
        Table icebergTable = catalog.loadTable(session, tableHandle.getSchemaTableName());

        return Optional.of(new IcebergTableExecuteHandle(
                tableHandle.getSchemaTableName(),
                ROLLBACK_TO_SNAPSHOT,
                new IcebergRollbackToSnapshotHandle(snapshotId),
                icebergTable.location(),
                tableHandle.getFormatVersion()));
    }

    private Optional<ConnectorTableExecuteHandle> getTableHandleForGenerateEmbeddings(
            ConnectorSession session,
            ConnectorAccessControl accessControl,
            IcebergTableHandle tableHandle,
            Map<String, Object> executeProperties,
            RetryMode retryMode)
    {
        String embeddingColumnName = (String) requireProcedureArgument(executeProperties, "embedding_column");
        String dataColumnName = (String) requireProcedureArgument(executeProperties, "data_column");
        String modelId = (String) requireProcedureArgument(executeProperties, "model_id");

        accessControl.checkCanSelectFromColumns(null, tableHandle.getSchemaTableName(), Set.of(embeddingColumnName, dataColumnName));
        accessControl.checkCanInsertIntoTable(null, tableHandle.getSchemaTableName());
        aiModelAccessControl.checkCanExecuteModel(new AiModelAccessControl.Context(session), modelId);

        Schema schema = SchemaParser.fromJson(tableHandle.getTableSchemaJson());
        Optional<NestedField> embeddingColumn = Optional.ofNullable(schema.caseInsensitiveFindField(embeddingColumnName));
        Optional<NestedField> dataColumn = Optional.ofNullable(schema.caseInsensitiveFindField(dataColumnName));

        checkProcedureArgument(embeddingColumn.isPresent(), "embedding_column does not exist: %s", embeddingColumnName);
        checkProcedureArgument(dataColumn.isPresent(), "data_column does not exist: %s", dataColumnName);
        TypeSignature embeddingTypeSignature = toTrinoType(embeddingColumn.get().type(), typeManager).getTypeSignature();
        checkProcedureArgument(
                toTrinoType(dataColumn.get().type(), typeManager).equals(VARCHAR),
                "data_column must reference a column with type VARCHAR");

        EmbeddingType embeddingType;
        if (embeddingTypeSignature.equals(TypeSignature.arrayType(RealType.REAL.getTypeSignature()))) {
            embeddingType = EmbeddingType.FLOAT;
        }
        else if (embeddingTypeSignature.equals(TypeSignature.arrayType(DoubleType.DOUBLE.getTypeSignature()))) {
            embeddingType = EmbeddingType.DOUBLE;
        }
        else if (embeddingTypeSignature.equals(VarbinaryType.VARBINARY.getTypeSignature())) {
            embeddingType = EmbeddingType.BINARY;
        }
        else {
            throw new TrinoException(INVALID_PROCEDURE_ARGUMENT, "embedding_column must reference a column with type ARRAY(DOUBLE), ARRAY(REAL), or VARBINARY");
        }

        Table icebergTable = catalog.loadTable(session, tableHandle.getSchemaTableName());
        int specId = tableHandle.getSpecId().orElseThrow(() -> new VerifyException("Partition spec missing in the table handle"));
        return Optional.of(new IcebergTableExecuteHandle(
                tableHandle.getSchemaTableName(),
                GENERATE_EMBEDDINGS,
                new IcebergGenerateEmbeddingsHandle(
                        modelId,
                        embeddingType,
                        embeddingColumn.get().fieldId(),
                        dataColumn.get().fieldId(),
                        tableHandle.getSnapshotId(),
                        tableHandle.getTableSchemaJson(),
                        specId,
                        tableHandle.getPartitionSpecJsons(),
                        getPartitionColumns(icebergTable, typeManager),
                        icebergTable.sortOrder().fields().stream()
                                .map(TrinoSortField::fromIceberg)
                                .collect(toImmutableList()),
                        getFileFormat(tableHandle.getStorageProperties()),
                        tableHandle.getStorageProperties(),
                        retryMode != NO_RETRIES),
                icebergTable.location(),
                tableHandle.getFormatVersion()));
    }

    private static Object requireProcedureArgument(Map<String, Object> properties, String name)
    {
        Object value = properties.get(name);
        checkProcedureArgument(value != null, "Required procedure argument '%s' is missing", name);
        return value;
    }

    @Override
    public Set<ColumnHandle> getColumnHandlesForTableExecute(ConnectorSession session, ConnectorTableHandle tableHandle, ConnectorTableExecuteHandle tableExecuteHandle)
    {
        IcebergTableExecuteHandle executeHandle = (IcebergTableExecuteHandle) tableExecuteHandle;
        IcebergTableHandle icebergTableHandle = (IcebergTableHandle) tableHandle;
        return switch (executeHandle.procedureId()) {
            case OPTIMIZE -> getColumnHandlesForOptimize(icebergTableHandle);
            case GENERATE_EMBEDDINGS -> getColumnHandlesForGenerateEmbeddings(icebergTableHandle);
            case OPTIMIZE_MANIFESTS, OPTIMIZE_POSITION_DELETES, DROP_EXTENDED_STATS, ROLLBACK_TO_SNAPSHOT, EXPIRE_SNAPSHOTS, REMOVE_ORPHAN_FILES, ADD_FILES, ADD_FILES_FROM_TABLE, REMOVE_DANGLING_DELETE_FILES ->
                    throw new IllegalArgumentException("Unknown procedure '" + executeHandle.procedureId() + "'");
        };
    }

    private Set<ColumnHandle> getColumnHandlesForOptimize(IcebergTableHandle table)
    {
        ImmutableSet.Builder<ColumnHandle> columnHandles = ImmutableSet.builder();
        for (IcebergColumnHandle columnHandle : getTopLevelColumns(SchemaParser.fromJson(table.getTableSchemaJson()), typeManager)) {
            columnHandles.add(columnHandle);
        }
        if (table.getFormatVersion() >= 3) {
            columnHandles.add(rowIdColumnHandle());
            columnHandles.add(lastUpdatedSequenceNumberColumnHandle());
        }
        return columnHandles.build();
    }

    private Set<ColumnHandle> getColumnHandlesForGenerateEmbeddings(IcebergTableHandle table)
    {
        return getTopLevelColumns(SchemaParser.fromJson(table.getTableSchemaJson()), typeManager).stream()
                .collect(toImmutableSet());
    }

    @Override
    public Optional<ConnectorTableLayout> getLayoutForTableExecute(ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle)
    {
        IcebergTableExecuteHandle executeHandle = (IcebergTableExecuteHandle) tableExecuteHandle;
        switch (executeHandle.procedureId()) {
            case OPTIMIZE, GENERATE_EMBEDDINGS:
                return getLayoutForOptimize(session, executeHandle);
            case OPTIMIZE_MANIFESTS:
            case OPTIMIZE_POSITION_DELETES:
            case REMOVE_DANGLING_DELETE_FILES:
            case DROP_EXTENDED_STATS:
            case ROLLBACK_TO_SNAPSHOT:
            case EXPIRE_SNAPSHOTS:
            case REMOVE_ORPHAN_FILES:
            case ADD_FILES:
            case ADD_FILES_FROM_TABLE:
                // handled via executeTableExecute
        }
        throw new IllegalArgumentException("Unknown procedure '" + executeHandle.procedureId() + "'");
    }

    private Optional<ConnectorTableLayout> getLayoutForOptimize(ConnectorSession session, IcebergTableExecuteHandle executeHandle)
    {
        Table icebergTable = catalog.loadTable(session, executeHandle.schemaTableName());
        // from performance perspective it is better to have lower number of bigger files than other way around
        // thus we force repartitioning for optimize to achieve this
        return getWriteLayout(icebergTable.schema(), icebergTable.spec(), true);
    }

    @Override
    public BeginTableExecuteResult<ConnectorTableExecuteHandle, ConnectorTableHandle> beginTableExecute(
            ConnectorSession session,
            ConnectorTableExecuteHandle tableExecuteHandle,
            ConnectorTableHandle updatedSourceTableHandle)
    {
        IcebergTableExecuteHandle executeHandle = (IcebergTableExecuteHandle) tableExecuteHandle;
        IcebergTableHandle table = (IcebergTableHandle) updatedSourceTableHandle;
        switch (executeHandle.procedureId()) {
            case OPTIMIZE:
                return beginOptimize(session, executeHandle, table);
            case GENERATE_EMBEDDINGS:
                return beginGenerateEmbeddings(session, executeHandle, table);
            case OPTIMIZE_MANIFESTS:
            case OPTIMIZE_POSITION_DELETES:
            case REMOVE_DANGLING_DELETE_FILES:
            case DROP_EXTENDED_STATS:
            case ROLLBACK_TO_SNAPSHOT:
            case EXPIRE_SNAPSHOTS:
            case REMOVE_ORPHAN_FILES:
            case ADD_FILES:
            case ADD_FILES_FROM_TABLE:
                // handled via executeTableExecute
        }
        throw new IllegalArgumentException("Unknown procedure '" + executeHandle.procedureId() + "'");
    }

    private BeginTableExecuteResult<ConnectorTableExecuteHandle, ConnectorTableHandle> beginOptimize(
            ConnectorSession session,
            IcebergTableExecuteHandle executeHandle,
            IcebergTableHandle table)
    {
        IcebergOptimizeHandle optimizeHandle = (IcebergOptimizeHandle) executeHandle.procedureHandle();
        BaseTable icebergTable = catalog.loadTable(session, table.getSchemaTableName());

        validateNotModifyingOldSnapshot(table, icebergTable);

        beginTransaction(icebergTable);

        return new BeginTableExecuteResult<>(
                executeHandle,
                table.forOptimize(true, optimizeHandle.maxScannedFileSize()));
    }

    private BeginTableExecuteResult<ConnectorTableExecuteHandle, ConnectorTableHandle> beginGenerateEmbeddings(
            ConnectorSession session,
            IcebergTableExecuteHandle executeHandle,
            IcebergTableHandle table)
    {
        Table icebergTable = catalog.loadTable(session, table.getSchemaTableName());
        validateNotModifyingOldSnapshot(table, icebergTable);
        int tableFormatVersion = formatVersion(icebergTable);
        if (tableFormatVersion > OPTIMIZE_MAX_SUPPORTED_TABLE_VERSION) {
            throw new TrinoException(NOT_SUPPORTED, format(
                    "generate_embeddings is not supported for Iceberg table format version > %d. Table %s format version is %s.",
                    OPTIMIZE_MAX_SUPPORTED_TABLE_VERSION,
                    table.getSchemaTableName(),
                    tableFormatVersion));
        }

        beginTransaction(icebergTable);
        return new BeginTableExecuteResult<>(executeHandle, table.forGenerateEmbeddings());
    }

    @Override
    public void finishTableExecute(ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle, Collection<Slice> fragments, List<Object> splitSourceInfo)
    {
        IcebergTableExecuteHandle executeHandle = (IcebergTableExecuteHandle) tableExecuteHandle;
        switch (executeHandle.procedureId()) {
            case OPTIMIZE:
                finishOptimize(session, executeHandle, fragments, splitSourceInfo);
                return;
            case GENERATE_EMBEDDINGS:
                finishGenerateEmbeddings(session, executeHandle, fragments, splitSourceInfo);
                return;
            case OPTIMIZE_MANIFESTS:
            case OPTIMIZE_POSITION_DELETES:
            case REMOVE_DANGLING_DELETE_FILES:
            case DROP_EXTENDED_STATS:
            case ROLLBACK_TO_SNAPSHOT:
            case EXPIRE_SNAPSHOTS:
            case REMOVE_ORPHAN_FILES:
            case ADD_FILES:
            case ADD_FILES_FROM_TABLE:
                // handled via executeTableExecute
        }
        throw new IllegalArgumentException("Unknown procedure '" + executeHandle.procedureId() + "'");
    }

    private void finishOptimize(ConnectorSession session, IcebergTableExecuteHandle executeHandle, Collection<Slice> fragments, List<Object> splitSourceInfo)
    {
        IcebergOptimizeHandle optimizeHandle = (IcebergOptimizeHandle) executeHandle.procedureHandle();
        Table icebergTable = transaction.table();

        // files to be deleted
        ImmutableSet.Builder<DataFile> scannedDataFilesBuilder = ImmutableSet.builder();
        ImmutableSet.Builder<DeleteFile> scannedDeleteFilesBuilder = ImmutableSet.builder();
        splitSourceInfo.stream().map(DataFileWithDeleteFiles.class::cast).forEach(dataFileWithDeleteFiles -> {
            scannedDataFilesBuilder.add(dataFileWithDeleteFiles.dataFile());
            scannedDeleteFilesBuilder.addAll(dataFileWithDeleteFiles.deleteFiles());
        });

        Set<DataFile> scannedDataFiles = scannedDataFilesBuilder.build();
        Set<DeleteFile> fullyAppliedDeleteFiles = scannedDeleteFilesBuilder.build();

        List<CommitTaskData> commitTasks = fragments.stream()
                .map(Slice::getInput)
                .map(commitTaskCodec::fromJson)
                .collect(toImmutableList());

        Type[] partitionColumnTypes = icebergTable.spec().fields().stream()
                .map(field -> field.transform().getResultType(
                        icebergTable.schema().findType(field.sourceId())))
                .toArray(Type[]::new);

        Set<DataFile> newFiles = new HashSet<>();
        Map<Integer, SortOrder> sortOrders = icebergTable.sortOrders();
        for (CommitTaskData task : commitTasks) {
            DataFiles.Builder builder = DataFiles.builder(icebergTable.spec())
                    .withPath(task.path())
                    .withFileSizeInBytes(task.fileSizeInBytes())
                    .withFormat(optimizeHandle.fileFormat().toIceberg())
                    .withMetrics(task.metrics().metrics())
                    .withSortOrder(sortOrders.get(task.sortOrderId()));
            task.fileSplitOffsets().ifPresent(builder::withSplitOffsets);

            if (!icebergTable.spec().fields().isEmpty()) {
                String partitionDataJson = task.partitionDataJson()
                        .orElseThrow(() -> new VerifyException("No partition data for partitioned table"));
                builder.withPartition(PartitionData.fromJson(partitionDataJson, partitionColumnTypes));
            }

            newFiles.add(builder.build());
        }

        if (optimizeHandle.snapshotId().isEmpty() || scannedDataFiles.isEmpty() && fullyAppliedDeleteFiles.isEmpty() && newFiles.isEmpty()) {
            // Either the table is empty, or the table scan turned out to be empty, nothing to commit
            transaction = null;
            return;
        }

        RewriteFiles rewriteFiles = transaction.newRewrite();
        scannedDataFiles.forEach(rewriteFiles::deleteFile);
        fullyAppliedDeleteFiles.forEach(rewriteFiles::deleteFile);
        newFiles.forEach(rewriteFiles::addFile);

        // Table.snapshot method returns null if there is no matching snapshot
        Snapshot snapshot = requireNonNull(icebergTable.snapshot(optimizeHandle.snapshotId().getAsLong()), "snapshot is null");
        // Set dataSequenceNumber to avoid contention between OPTIMIZE and concurrent writing of equality deletes
        rewriteFiles.dataSequenceNumber(snapshot.sequenceNumber());
        rewriteFiles.validateFromSnapshot(snapshot.snapshotId());
        rewriteFiles.scanManifestsWith(icebergScanExecutor);
        commitUpdate(rewriteFiles, session, "optimize");

        long newSnapshotId = icebergTable.currentSnapshot().snapshotId();
        StatisticsFile newStatsFile = tableStatisticsWriter.rewriteStatisticsFile(session, icebergTable, newSnapshotId);
        transaction.updateStatistics()
                .setStatistics(newStatsFile)
                .commit();
        partitionStatisticsWriter.writePartitionStats(session, icebergTable, newSnapshotId).ifPresent(partitionStatisticsFile -> {
            transaction.updatePartitionStatistics()
                    .setPartitionStatistics(partitionStatisticsFile)
                    .commit();
        });

        commitTransaction(transaction, "optimize");
        transaction = null;
    }

    private void finishGenerateEmbeddings(ConnectorSession session, IcebergTableExecuteHandle executeHandle, Collection<Slice> fragments, List<Object> splitSourceInfo)
    {
        IcebergGenerateEmbeddingsHandle generateEmbeddingsHandle = (IcebergGenerateEmbeddingsHandle) executeHandle.procedureHandle();
        Table icebergTable = transaction.table();

        // files to be deleted
        ImmutableSet.Builder<DataFile> scannedDataFilesBuilder = ImmutableSet.builder();
        ImmutableSet.Builder<DeleteFile> scannedDeleteFilesBuilder = ImmutableSet.builder();
        splitSourceInfo.stream().map(DataFileWithDeleteFiles.class::cast).forEach(dataFileWithDeleteFiles -> {
            scannedDataFilesBuilder.add(dataFileWithDeleteFiles.dataFile());
            scannedDeleteFilesBuilder.addAll(dataFileWithDeleteFiles.deleteFiles());
        });

        Set<DataFile> scannedDataFiles = scannedDataFilesBuilder.build();
        Set<DeleteFile> fullyAppliedDeleteFiles = scannedDeleteFilesBuilder.build();

        List<CommitTaskData> commitTasks = fragments.stream()
                .map(Slice::getInput)
                .map(commitTaskCodec::fromJson)
                .collect(toImmutableList());

        Type[] partitionColumnTypes = icebergTable.spec().fields().stream()
                .map(field -> field.transform().getResultType(
                        icebergTable.schema().findType(field.sourceId())))
                .toArray(Type[]::new);

        Set<DataFile> newFiles = new HashSet<>();
        for (CommitTaskData task : commitTasks) {
            DataFiles.Builder builder = DataFiles.builder(icebergTable.spec())
                    .withPath(task.path())
                    .withFileSizeInBytes(task.fileSizeInBytes())
                    .withFormat(generateEmbeddingsHandle.fileFormat().toIceberg())
                    .withMetrics(task.metrics().metrics());
            task.fileSplitOffsets().ifPresent(builder::withSplitOffsets);

            if (!icebergTable.spec().fields().isEmpty()) {
                String partitionDataJson = task.partitionDataJson()
                        .orElseThrow(() -> new VerifyException("No partition data for partitioned table"));
                builder.withPartition(PartitionData.fromJson(partitionDataJson, partitionColumnTypes));
            }

            newFiles.add(builder.build());
        }

        if (generateEmbeddingsHandle.snapshotId().isEmpty() || scannedDataFiles.isEmpty() && fullyAppliedDeleteFiles.isEmpty() && newFiles.isEmpty()) {
            // Either the table is empty, or the table scan turned out to be empty, nothing to commit
            transaction = null;
            return;
        }

        RewriteFiles rewriteFiles = transaction.newRewrite();
        scannedDataFiles.forEach(rewriteFiles::deleteFile);
        fullyAppliedDeleteFiles.forEach(rewriteFiles::deleteFile);
        newFiles.forEach(rewriteFiles::addFile);

        // Table.snapshot method returns null if there is no matching snapshot
        Snapshot snapshot = requireNonNull(icebergTable.snapshot(generateEmbeddingsHandle.snapshotId().getAsLong()), "snapshot is null");
        rewriteFiles.validateFromSnapshot(snapshot.snapshotId());
        rewriteFiles.scanManifestsWith(icebergScanExecutor);
        commitUpdate(rewriteFiles, session, "generate_embeddings");

        long newSnapshotId = icebergTable.currentSnapshot().snapshotId();
        StatisticsFile newStatsFile = tableStatisticsWriter.rewriteStatisticsFile(session, icebergTable, newSnapshotId);

        transaction.updateStatistics()
                    .setStatistics(newStatsFile)
                    .commit();

        partitionStatisticsWriter.writePartitionStats(session, icebergTable, newSnapshotId).ifPresent(partitionStatisticsFile -> {
            transaction.updatePartitionStatistics()
                    .setPartitionStatistics(partitionStatisticsFile)
                    .commit();
        });

        commitTransaction(transaction, "update statistics after generate_embeddings");
        transaction = null;
    }

    private static void commitUpdateAndTransaction(SnapshotUpdate<?> update, ConnectorSession session, Transaction transaction, String operation)
    {
        commitUpdate(update, session, operation);
        commitTransaction(transaction, operation);
    }

    private static void commitUpdate(SnapshotUpdate<?> update, ConnectorSession session, String operation)
    {
        try {
            commit(update, session);
        }
        catch (UncheckedIOException | ValidationException | CommitFailedException | CommitStateUnknownException | RESTException e) {
            throw new TrinoException(ICEBERG_COMMIT_ERROR, format("Failed to commit during %s: %s", operation, requireNonNullElse(e.getMessage(), e)), e);
        }
    }

    private static void commitTransaction(Transaction transaction, String operation)
    {
        try {
            transaction.commitTransaction();
        }
        catch (UncheckedIOException | ValidationException | CommitFailedException | CommitStateUnknownException | RESTException e) {
            throw new TrinoException(ICEBERG_COMMIT_ERROR, format("Failed to commit the transaction during %s: %s", operation, requireNonNullElse(e.getMessage(), e)), e);
        }
    }

    @Override
    public Map<String, Long> executeTableExecute(ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle)
    {
        IcebergTableExecuteHandle executeHandle = (IcebergTableExecuteHandle) tableExecuteHandle;
        switch (executeHandle.procedureId()) {
            case OPTIMIZE_MANIFESTS:
                return executeOptimizeManifests(session, executeHandle);
            case OPTIMIZE_POSITION_DELETES:
                return executeOptimizePositionDeletes(session, executeHandle);
            case REMOVE_DANGLING_DELETE_FILES:
                return executeRemoveDanglingDeleteFiles(session, executeHandle);
            case DROP_EXTENDED_STATS:
                executeDropExtendedStats(session, executeHandle);
                return ImmutableMap.of();
            case ROLLBACK_TO_SNAPSHOT:
                executeRollbackToSnapshot(session, executeHandle);
                return ImmutableMap.of();
            case EXPIRE_SNAPSHOTS:
                IcebergExpireSnapshotsHandle icebergExpireSnapshotsHandle = (IcebergExpireSnapshotsHandle) executeHandle.procedureHandle();
                executeExpireSnapshots(
                        session,
                        executeHandle.schemaTableName(),
                        icebergExpireSnapshotsHandle.retentionThreshold(),
                        getExpireSnapshotMinRetention(session),
                        icebergExpireSnapshotsHandle.retainLast(),
                        icebergExpireSnapshotsHandle.cleanExpiredMetadata());
                return ImmutableMap.of();
            case REMOVE_ORPHAN_FILES:
                return executeRemoveOrphanFiles(session, executeHandle);
            case ADD_FILES:
                executeAddFiles(session, executeHandle);
                return ImmutableMap.of();
            case ADD_FILES_FROM_TABLE:
                executeAddFilesFromTable(session, executeHandle);
                return ImmutableMap.of();
            default:
                throw new IllegalArgumentException("Unknown procedure '" + executeHandle.procedureId() + "'");
        }
    }

    private Map<String, Long> executeOptimizeManifests(ConnectorSession session, IcebergTableExecuteHandle executeHandle)
    {
        checkArgument(executeHandle.procedureHandle() instanceof IcebergOptimizeManifestsHandle, "Unexpected procedure handle %s", executeHandle.procedureHandle());

        BaseTable icebergTable = catalog.loadTable(session, executeHandle.schemaTableName());
        // org.apache.iceberg.BaseRewriteManifests currently rewrites only data manifests
        List<ManifestFile> manifests = loadDataManifestsFromSnapshot(icebergTable, icebergTable.currentSnapshot());
        if (manifests.isEmpty()) {
            return ImmutableMap.of();
        }
        long manifestTargetSizeBytes = icebergTable.operations().current().propertyAsLong(MANIFEST_TARGET_SIZE_BYTES, MANIFEST_TARGET_SIZE_BYTES_DEFAULT);
        if (manifests.size() == 1 && manifests.getFirst().length() < manifestTargetSizeBytes) {
            return ImmutableMap.of();
        }
        long totalManifestsSize = manifests.stream().mapToLong(ManifestFile::length).sum();
        // Having too many open manifest writers can potentially cause OOM on the coordinator
        int targetManifestClusters = toIntExact(Math.min(((totalManifestsSize + manifestTargetSizeBytes - 1) / manifestTargetSizeBytes), 200));

        // Cluster manifests by partitioning fields for better manifest pruning when reading data files with filters
        Map<Object, Integer> clusteredPartitionValues;
        if (icebergTable.spec().isPartitioned() && targetManifestClusters > 1) {
            clusteredPartitionValues = getClusteredPartitionValues(icebergTable, manifests, targetManifestClusters);
        }
        else {
            clusteredPartitionValues = ImmutableMap.of();
        }

        Types.StructType partitionType = icebergTable.spec().partitionType();
        Map<Integer, PartitionSpec> specs = icebergTable.specs();
        CommitMetricsReporter reporter = new CommitMetricsReporter();
        icebergTable = new BaseTable(icebergTable.operations(), icebergTable.name(), MetricsReporters.combine(icebergTable.reporter(), reporter));
        RewriteManifests rewriteManifests = icebergTable.rewriteManifests();
        // commit.manifest.target-size-bytes is enforced by RewriteManifests,
        // so we don't have to worry about any manifest file getting too big
        // because of the clustering logic
        rewriteManifests
                .clusterBy(dataFile -> {
                    if (clusteredPartitionValues.isEmpty()) {
                        return 0;
                    }
                    StructLike partition = PartitionUtil.coercePartition(partitionType, specs.get(dataFile.specId()), dataFile.partition());
                    Object value = partition.get(0, Object.class);
                    if (value == null) {
                        return 0;
                    }
                    return clusteredPartitionValues.get(value);
                })
                .scanManifestsWith(icebergScanExecutor)
                .commit();

        CommitReport report = reporter.commitReport();
        if (report == null) {
            return ImmutableMap.of();
        }
        log.info("optimize_manifests on table %s, commit report %s", icebergTable.name(), report);

        CommitMetricsResult metrics = report.commitMetrics();
        if (metrics == null) {
            return ImmutableMap.of();
        }
        return ImmutableMap.of(
                "rewritten_manifests_count", metrics.manifestsReplaced().value(),
                "added_manifests_count", metrics.manifestsCreated().value(),
                "kept_manifests_count", metrics.manifestsKept().value(),
                "processed_manifest_entries_count", metrics.manifestEntriesProcessed().value());
    }

    private Map<Object, Integer> getClusteredPartitionValues(
            BaseTable icebergTable,
            List<ManifestFile> dataManifests,
            int targetClusters)
    {
        checkArgument(icebergTable.spec().isPartitioned(), "Table %s must be partitioned", icebergTable);

        // Clustering is limited to the top-level partitioning column as that is likely
        // to be the most effective for filters during reads
        Type.PrimitiveType firstPartitionFieldType = icebergTable.spec().partitionType()
                .fields().getFirst()
                .type().asPrimitiveType();
        Hash.Strategy<Object> icebergHashStrategy = new IcebergHashStrategy(firstPartitionFieldType);

        Iterable<CloseableIterable<ContentFile<DataFile>>> dataFileIterables = Iterables.transform(
                dataManifests,
                manifestFile ->
                        CloseableIterable.transform(
                                liveEntries(
                                        ManifestFiles.read(manifestFile, icebergTable.io(), icebergTable.specs()).select(ImmutableList.of("partition"))),
                                ContentFile::copyWithoutStats));
        // Collect unique values of top-level partitioning column
        Set<Object> uniqueValues = new ObjectOpenCustomHashSet<>(icebergHashStrategy);
        try (CloseableIterable<ContentFile<DataFile>> dataFiles = new ParallelIterable<>(dataFileIterables, icebergScanExecutor)) {
            Types.StructType partitionType = icebergTable.spec().partitionType();
            Map<Integer, PartitionSpec> specs = icebergTable.specs();
            dataFiles.forEach(dataFile -> {
                StructLike partition = PartitionUtil.coercePartition(partitionType, specs.get(dataFile.specId()), dataFile.partition());
                Object value = partition.get(0, Object.class);
                if (value != null) {
                    uniqueValues.add(value);
                }
            });
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        if (uniqueValues.isEmpty()) {
            return ImmutableMap.of();
        }
        // Sort unique values to cluster nearby partitions together
        Object[] sortedValues = uniqueValues.toArray();
        ObjectArrays.quickSort(sortedValues, Comparators.forType(firstPartitionFieldType));

        // Assign a bucket in range [0, targetClusters) to the partition values, while also grouping
        // adjacent values into same bucket
        Map<Object, Integer> valueToBucket = new Object2IntOpenCustomHashMap<>(icebergHashStrategy);
        for (int i = 0; i < sortedValues.length; i++) {
            int bucket = (i * targetClusters / sortedValues.length);
            valueToBucket.put(sortedValues[i], bucket);
        }

        return valueToBucket;
    }

    /**
     * Bridges Iceberg's JavaHash and Comparator into FastUtil's Hash.Strategy.
     */
    private static class IcebergHashStrategy
            implements Hash.Strategy<Object>
    {
        private final JavaHash<Object> hasher;
        private final Comparator<Object> comparator;

        IcebergHashStrategy(Type.PrimitiveType type)
        {
            this.hasher = JavaHash.forType(type);
            this.comparator = Comparators.forType(type);
        }

        @Override
        public int hashCode(Object o)
        {
            return o == null ? 0 : hasher.hash(o);
        }

        @Override
        public boolean equals(Object a, Object b)
        {
            if (a == b) {
                return true;
            }
            if (a == null || b == null) {
                return false;
            }
            return comparator.compare(a, b) == 0;
        }
    }

    private Map<String, Long> executeOptimizePositionDeletes(ConnectorSession session, IcebergTableExecuteHandle executeHandle)
    {
        checkArgument(executeHandle.procedureHandle() instanceof IcebergOptimizePositionDeletesHandle, "Unexpected procedure handle %s", executeHandle.procedureHandle());

        BaseTable icebergTable = catalog.loadTable(session, executeHandle.schemaTableName());

        return optimizePositionDeletes.execute(session, icebergTable, executeHandle.schemaTableName());
    }

    private Map<String, Long> executeRemoveDanglingDeleteFiles(ConnectorSession session, IcebergTableExecuteHandle executeHandle)
    {
        checkArgument(executeHandle.procedureHandle() instanceof IcebergRemoveDanglingDeleteFilesHandle, "Unexpected procedure handle %s", executeHandle.procedureHandle());

        try {
            BaseTable icebergTable = catalog.loadTable(session, executeHandle.schemaTableName());
            try (var _ = icebergTable.io()) {
                DanglingDeleteFilesResult danglingDeleteFilesResult = removeDanglingDeleteFiles.collectDanglingDeleteFiles(icebergTable);

                if (danglingDeleteFilesResult.dangingDeleteFiles().isEmpty()) {
                    return ImmutableMap.of(
                            "removed_delete_files_count", 0L,
                            "dangling_equality_delete_files_count", 0L,
                            "dangling_position_delete_files_count", 0L,
                            "dangling_dv_files_count", 0L,
                            "data_files_without_sequence_numbers", 0L,
                            "unexpected_delete_files_count", 0L);
                }

                Snapshot currentSnapshot = icebergTable.currentSnapshot();
                beginTransaction(icebergTable);
                RowDelta rowDelta = transaction.newRowDelta();
                for (DeleteFile file : danglingDeleteFilesResult.dangingDeleteFiles()) {
                    rowDelta.removeDeletes(file);
                }
                rowDelta.validateFromSnapshot(currentSnapshot.snapshotId());
                commitUpdate(rowDelta, session, "remove_dangling_delete_files");
                commitTransaction(transaction, "remove_dangling_delete_files");
                transaction = null;

                return ImmutableMap.of(
                        "removed_delete_files_count", (long) danglingDeleteFilesResult.dangingDeleteFiles().size(),
                        "dangling_equality_delete_files_count", danglingDeleteFilesResult.metrics().getDanglingEqualityDeleteFilesCount(),
                        "dangling_position_delete_files_count", danglingDeleteFilesResult.metrics().getDanglingPositionDeleteFilesCount(),
                        "dangling_dv_files_count", danglingDeleteFilesResult.metrics().getDanglingDvFilesCount(),
                        "data_files_without_sequence_numbers", danglingDeleteFilesResult.metrics().getDataFilesWithoutSequenceNumbers(),
                        "unexpected_delete_files_count", danglingDeleteFilesResult.metrics().getUnexpectedDeleteFilesCount());
            }
        }
        catch (NotFoundException e) {
            throw new TrinoException(ICEBERG_INVALID_METADATA, e);
        }
    }

    private void executeDropExtendedStats(ConnectorSession session, IcebergTableExecuteHandle executeHandle)
    {
        checkArgument(executeHandle.procedureHandle() instanceof IcebergDropExtendedStatsHandle, "Unexpected procedure handle %s", executeHandle.procedureHandle());

        try {
            Table icebergTable = catalog.loadTable(session, executeHandle.schemaTableName());
            beginTransaction(icebergTable);
            UpdateStatistics updateStatistics = transaction.updateStatistics();
            for (StatisticsFile statisticsFile : icebergTable.statisticsFiles()) {
                updateStatistics.removeStatistics(statisticsFile.snapshotId());
            }
            updateStatistics.commit();
            commitTransaction(transaction, "drop extended stats");
        }
        catch (NotFoundException e) {
            throw new TrinoException(ICEBERG_INVALID_METADATA, e);
        }
        transaction = null;
    }

    private void executeRollbackToSnapshot(ConnectorSession session, IcebergTableExecuteHandle executeHandle)
    {
        checkArgument(executeHandle.procedureHandle() instanceof IcebergRollbackToSnapshotHandle, "Unexpected procedure handle %s", executeHandle.procedureHandle());
        long snapshotId = ((IcebergRollbackToSnapshotHandle) executeHandle.procedureHandle()).snapshotId();

        try {
            Table icebergTable = catalog.loadTable(session, executeHandle.schemaTableName());
            icebergTable.manageSnapshots().setCurrentSnapshot(snapshotId).commit();
        }
        catch (NotFoundException e) {
            throw new TrinoException(ICEBERG_INVALID_METADATA, e);
        }
    }

    private void executeExpireSnapshots(
            ConnectorSession session,
            SchemaTableName schemaTableName,
            Duration retentionThreshold,
            Duration minimumAllowedRetention,
            int retainLast,
            boolean cleanExpiredMetadata)
    {
        BaseTable table = catalog.loadTable(session, schemaTableName);
        Duration retention = requireNonNull(retentionThreshold, "retention is null");
        validateTableExecuteParameters(
                table,
                schemaTableName,
                EXPIRE_SNAPSHOTS.name(),
                retention,
                minimumAllowedRetention,
                IcebergConfig.EXPIRE_SNAPSHOTS_MIN_RETENTION,
                IcebergSessionProperties.EXPIRE_SNAPSHOTS_MIN_RETENTION);

        // ForwardingFileIo handles bulk operations so no separate function implementation is needed
        try {
            table.expireSnapshots()
                    .expireOlderThan(session.getStart().toEpochMilli() - retention.toMillis())
                    .retainLast(retainLast)
                    .cleanExpiredMetadata(cleanExpiredMetadata)
                    .planWith(icebergScanExecutor)
                    .commit();
        }
        catch (NotFoundException e) {
            throw new TrinoException(ICEBERG_INVALID_METADATA, e);
        }
    }

    private static void validateTableExecuteParameters(
            BaseTable table,
            SchemaTableName schemaTableName,
            String procedureName,
            Duration retentionThreshold,
            Duration minRetention,
            String minRetentionParameterName,
            String sessionMinRetentionParameterName)
    {
        int tableFormatVersion = formatVersion(table);
        if (tableFormatVersion > CLEANING_UP_PROCEDURES_MAX_SUPPORTED_TABLE_VERSION) {
            // It is not known if future version won't bring any new kind of metadata or data files
            // because of the way procedures are implemented it is safer to fail here than to potentially remove
            // files that should stay there
            throw new TrinoException(NOT_SUPPORTED, format("%s is not supported for Iceberg table format version > %d. " +
                            "Table %s format version is %s.",
                    procedureName,
                    CLEANING_UP_PROCEDURES_MAX_SUPPORTED_TABLE_VERSION,
                    schemaTableName,
                    tableFormatVersion));
        }
        Map<String, String> properties = table.properties();
        if (properties.containsKey(WRITE_LOCATION_PROVIDER_IMPL)) {
            throw new TrinoException(NOT_SUPPORTED, "Table " + schemaTableName + " specifies " + properties.get(WRITE_LOCATION_PROVIDER_IMPL) +
                    " as a location provider. Writing to Iceberg tables with custom location provider is not supported.");
        }

        Duration retention = requireNonNull(retentionThreshold, "retention is null");
        checkProcedureArgument(retention.compareTo(minRetention) >= 0,
                "Retention specified (%s) is shorter than the minimum retention configured in the system (%s). " +
                        "Minimum retention can be changed with %s configuration property or iceberg.%s session property",
                retention,
                minRetention,
                minRetentionParameterName,
                sessionMinRetentionParameterName);
    }

    public Map<String, Long> executeRemoveOrphanFiles(ConnectorSession session, IcebergTableExecuteHandle executeHandle)
    {
        IcebergRemoveOrphanFilesHandle removeOrphanFilesHandle = (IcebergRemoveOrphanFilesHandle) executeHandle.procedureHandle();

        BaseTable table = catalog.loadTable(session, executeHandle.schemaTableName());
        Duration retention = requireNonNull(removeOrphanFilesHandle.retentionThreshold(), "retention is null");
        validateTableExecuteParameters(
                table,
                executeHandle.schemaTableName(),
                REMOVE_ORPHAN_FILES.name(),
                retention,
                getRemoveOrphanFilesMinRetention(session),
                IcebergConfig.REMOVE_ORPHAN_FILES_MIN_RETENTION,
                IcebergSessionProperties.REMOVE_ORPHAN_FILES_MIN_RETENTION);

        if (table.currentSnapshot() == null) {
            log.debug("Skipping remove_orphan_files procedure for empty table %s", table);
            return ImmutableMap.of();
        }

        Instant expiration = session.getStart().minusMillis(retention.toMillis());
        return removeOrphanFiles(table, session, executeHandle.schemaTableName(), expiration, table.io().properties());
    }

    private Map<String, Long> removeOrphanFiles(Table table, ConnectorSession session, SchemaTableName schemaTableName, Instant expiration, Map<String, String> fileIoProperties)
    {
        Set<String> processedManifestFilePaths = new HashSet<>();
        // Similarly to issues like https://github.com/trinodb/trino/issues/13759, equivalent paths may have different String
        // representations due to things like double slashes. Using file names may result in retaining files which could be removed.
        // However, in practice Iceberg metadata and data files have UUIDs in their names which makes this unlikely.
        Set<String> validFileNames = Sets.newConcurrentHashSet();
        List<Future<?>> manifestScanFutures = new ArrayList<>();

        for (Snapshot snapshot : table.snapshots()) {
            String manifestListLocation = snapshot.manifestListLocation();
            List<ManifestFile> allManifests;
            if (manifestListLocation != null) {
                validFileNames.add(fileName(manifestListLocation));
                allManifests = loadAllManifestsFromManifestList(table, manifestListLocation);
            }
            else {
                // This is to maintain support for V1 tables which have embedded manifest lists
                allManifests = loadAllManifestsFromSnapshot(table, snapshot);
            }

            for (ManifestFile manifest : allManifests) {
                if (!processedManifestFilePaths.add(manifest.path())) {
                    // Already read this manifest
                    continue;
                }

                validFileNames.add(fileName(manifest.path()));
                manifestScanFutures.add(icebergScanExecutor.submit(() -> {
                    try (ManifestReader<? extends ContentFile<?>> manifestReader = readerForManifest(manifest, table)) {
                        for (ContentFile<?> contentFile : manifestReader.select(ImmutableList.of("file_path"))) {
                            validFileNames.add(fileName(contentFile.location()));
                        }
                    }
                    catch (IOException | UncheckedIOException e) {
                        throw new TrinoException(ICEBERG_FILESYSTEM_ERROR, "Unable to list manifest file content from " + manifest.path(), e);
                    }
                    catch (NotFoundException e) {
                        throw new TrinoException(ICEBERG_INVALID_METADATA, "Manifest file does not exist: " + manifest.path());
                    }
                }));
            }
        }

        metadataFileLocations(table, false).stream()
                .map(IcebergUtil::fileName)
                .forEach(validFileNames::add);

        // statisticsFilesLocations includes both table-level and partition-level statistics files
        statisticsFilesLocations(table).stream()
                .map(IcebergUtil::fileName)
                .forEach(validFileNames::add);

        validFileNames.add("version-hint.text");

        try {
            manifestScanFutures.forEach(MoreFutures::getFutureValue);
            // All futures completed normally
            manifestScanFutures.clear();
        }
        finally {
            // Ensure any futures still running are canceled in case of failure
            manifestScanFutures.forEach(future -> future.cancel(true));
        }
        ScanAndDeleteResult result = scanAndDeleteInvalidFiles(table, session, schemaTableName, expiration, validFileNames, fileIoProperties);
        log.info("remove_orphan_files for table %s processed %d manifest files, found %d active files, scanned %d files, deleted %d files",
                schemaTableName,
                processedManifestFilePaths.size(),
                validFileNames.size() - 1, // excluding version-hint.text
                result.scannedFilesCount(),
                result.deletedFilesCount());
        return ImmutableMap.of(
                "processed_manifests_count", (long) processedManifestFilePaths.size(),
                "active_files_count", (long) validFileNames.size() - 1, // excluding version-hint.text
                "scanned_files_count", result.scannedFilesCount(),
                "deleted_files_count", result.deletedFilesCount());
    }

    public void executeAddFiles(ConnectorSession session, IcebergTableExecuteHandle executeHandle)
    {
        IcebergAddFilesHandle addFilesHandle = (IcebergAddFilesHandle) executeHandle.procedureHandle();
        Table table = catalog.loadTable(session, executeHandle.schemaTableName());
        TrinoFileSystem fileSystem = fileSystemFactory.create(session.getIdentity(), table.io().properties());
        addFiles(
                session,
                fileSystem,
                catalog,
                executeHandle.schemaTableName(),
                addFilesHandle.location(),
                addFilesHandle.format(),
                addFilesHandle.recursiveDirectory(),
                icebergScanExecutor);
    }

    public void executeAddFilesFromTable(ConnectorSession session, IcebergTableExecuteHandle executeHandle)
    {
        IcebergAddFilesFromTableHandle addFilesHandle = (IcebergAddFilesFromTableHandle) executeHandle.procedureHandle();
        Table table = catalog.loadTable(session, executeHandle.schemaTableName());
        TrinoFileSystem fileSystem = fileSystemFactory.create(session.getIdentity(), table.io().properties());
        addFilesFromTable(
                session,
                fileSystem,
                metastoreFactory.orElseThrow(),
                table,
                addFilesHandle.table(),
                addFilesHandle.partitionFilter(),
                addFilesHandle.recursiveDirectory(),
                icebergScanExecutor);
    }

    private ScanAndDeleteResult scanAndDeleteInvalidFiles(Table table, ConnectorSession session, SchemaTableName schemaTableName, Instant expiration, Set<String> validFiles, Map<String, String> fileIoProperties)
    {
        List<Future<?>> deleteFutures = new ArrayList<>();
        long scannedFilesCount = 0;
        long deletedFilesCount = 0;
        try {
            List<Location> filesToDelete = new ArrayList<>(DELETE_BATCH_SIZE);
            TrinoFileSystem fileSystem = fileSystemFactory.create(session.getIdentity(), fileIoProperties);
            FileIterator allFiles = fileSystem.listFiles(Location.of(table.location()));
            while (allFiles.hasNext()) {
                FileEntry entry = allFiles.next();
                scannedFilesCount++;
                if (entry.lastModified().isBefore(expiration) && !validFiles.contains(entry.location().fileName())) {
                    filesToDelete.add(entry.location());
                    deletedFilesCount++;
                    if (filesToDelete.size() >= DELETE_BATCH_SIZE) {
                        List<Location> finalFilesToDelete = filesToDelete;
                        deleteFutures.add(icebergFileDeleteExecutor.submit(() -> deleteFiles(finalFilesToDelete, schemaTableName, fileSystem)));
                        filesToDelete = new ArrayList<>(DELETE_BATCH_SIZE);
                    }
                }
                else {
                    log.debug("%s file retained while removing orphan files %s", entry.location(), schemaTableName.getTableName());
                }
            }
            if (!filesToDelete.isEmpty()) {
                log.debug("Deleting files while removing orphan files for table %s %s", schemaTableName, filesToDelete);
                fileSystem.deleteFiles(filesToDelete);
            }

            deleteFutures.forEach(MoreFutures::getFutureValue);
            // All futures completed normally
            deleteFutures.clear();
        }
        catch (IOException | UncheckedIOException e) {
            throw new TrinoException(ICEBERG_FILESYSTEM_ERROR, "Failed removing orphan files for table: " + schemaTableName, e);
        }
        finally {
            // Ensure any futures still running are canceled in case of failure
            deleteFutures.forEach(future -> future.cancel(true));
        }
        return new ScanAndDeleteResult(scannedFilesCount, deletedFilesCount);
    }

    private record ScanAndDeleteResult(long scannedFilesCount, long deletedFilesCount) {}

    private void deleteFiles(List<Location> files, SchemaTableName schemaTableName, TrinoFileSystem fileSystem)
    {
        log.debug("Deleting files while removing orphan files for table %s [%s]", schemaTableName, files);
        try {
            fileSystem.deleteFiles(files);
        }
        catch (IOException | UncheckedIOException e) {
            throw new TrinoException(ICEBERG_FILESYSTEM_ERROR, "Failed removing orphan files for table: " + schemaTableName, e);
        }
    }

    @Override
    public FunctionDependencyDeclaration getFunctionDependencies(ConnectorSession session, FunctionId functionId, BoundSignature boundSignature)
    {
        return FunctionDependencyDeclaration.NO_DEPENDENCIES;
    }

    @Override
    public Optional<Object> getInfo(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle icebergTableHandle = (IcebergTableHandle) tableHandle;
        List<String> partitionFields = icebergTableHandle.getSpecId().isPresent() ?
                PartitionSpecParser.fromJson(SchemaParser.fromJson(icebergTableHandle.getTableSchemaJson()), icebergTableHandle.getPartitionSpecJsons().get(icebergTableHandle.getSpecId().getAsInt()))
                        .fields().stream()
                        .map(field -> field.name() + ": " + field.transform())
                        .collect(toImmutableList()) : ImmutableList.of();

        Map<String, String> summary = ImmutableMap.of();
        if (icebergTableHandle.getSnapshotId().isPresent()) {
            Table table = catalog.loadTable(session, icebergTableHandle.getSchemaTableName());
            summary = table.snapshot(icebergTableHandle.getSnapshotId().getAsLong()).summary();
        }
        Optional<String> totalRecords = Optional.ofNullable(summary.get(TOTAL_RECORDS_PROP));
        Optional<String> deletedRecords = Optional.ofNullable(summary.get(DELETED_RECORDS_PROP));
        Optional<String> totalDataFiles = Optional.ofNullable(summary.get(TOTAL_DATA_FILES_PROP));
        Optional<String> totalDeleteFiles = Optional.ofNullable(summary.get(TOTAL_DELETE_FILES_PROP));
        Optional<String> totalPositionDeletes = Optional.ofNullable(summary.get(TOTAL_POS_DELETES_PROP));
        Optional<String> totalEqualityDeletes = Optional.ofNullable(summary.get(TOTAL_EQ_DELETES_PROP));

        return Optional.of(new IcebergInputInfo(
                icebergTableHandle.getFormatVersion(),
                icebergTableHandle.getSnapshotId(),
                partitionFields,
                getFileFormat(icebergTableHandle.getStorageProperties()).name(),
                totalRecords,
                deletedRecords,
                totalDataFiles,
                totalDeleteFiles,
                totalPositionDeletes,
                totalEqualityDeletes));
    }

    @Override
    public Metrics getMetrics(ConnectorSession session)
    {
        return catalog.getMetrics();
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        if (tableHandle instanceof CorruptedIcebergTableHandle corruptedTableHandle) {
            catalog.dropCorruptedTable(session, corruptedTableHandle.schemaTableName());
        }
        else {
            catalog.dropTable(session, ((IcebergTableHandle) tableHandle).getSchemaTableName());
        }
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTable)
    {
        IcebergTableHandle handle = checkValidTableHandle(tableHandle);
        catalog.renameTable(session, handle.getSchemaTableName(), newTable);
    }

    @Override
    public void setTableProperties(ConnectorSession session, ConnectorTableHandle tableHandle, Map<String, Optional<Object>> properties)
    {
        IcebergTableHandle table = checkValidTableHandle(tableHandle);
        Table icebergTable = catalog.loadTable(session, table.getSchemaTableName());

        Set<String> unsupportedProperties = difference(properties.keySet(), UPDATABLE_TABLE_PROPERTIES);
        if (!unsupportedProperties.isEmpty()) {
            throw new TrinoException(NOT_SUPPORTED, "The following properties cannot be updated: " + String.join(", ", unsupportedProperties));
        }

        beginTransaction(icebergTable);
        UpdateProperties updateProperties = transaction.updateProperties();

        if (properties.containsKey(EXTRA_PROPERTIES_PROPERTY)) {
            @SuppressWarnings("unchecked")
            Map<String, String> extraProperties = (Map<String, String>) properties.get(EXTRA_PROPERTIES_PROPERTY)
                    .orElseThrow(() -> new IllegalArgumentException("The extra_properties property cannot be empty"));
            verifyExtraProperties(properties.keySet(), extraProperties, allowedExtraProperties);
            extraProperties.forEach(updateProperties::set);
        }

        if (properties.containsKey(PARQUET_BLOOM_FILTER_COLUMNS_PROPERTY)) {
            checkFormatForProperty(getFileFormat(icebergTable).toIceberg(), FileFormat.PARQUET, PARQUET_BLOOM_FILTER_COLUMNS_PROPERTY);
            @SuppressWarnings("unchecked")
            List<String> parquetBloomFilterColumns = (List<String>) properties.get(PARQUET_BLOOM_FILTER_COLUMNS_PROPERTY)
                    .orElseThrow(() -> new IllegalArgumentException("The parquet_bloom_filter_columns property cannot be empty"));
            validateParquetBloomFilterColumns(getColumnMetadatas(SchemaParser.fromJson(table.getTableSchemaJson()), typeManager, table.getFormatVersion()), parquetBloomFilterColumns);

            Set<String> existingParquetBloomFilterColumns = icebergTable.properties().keySet().stream()
                    .filter(key -> key.startsWith(PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX))
                    .map(key -> key.substring(PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX.length()))
                    .collect(toImmutableSet());
            Set<String> removeParquetBloomFilterColumns = Sets.difference(existingParquetBloomFilterColumns, Set.copyOf(parquetBloomFilterColumns));
            removeParquetBloomFilterColumns.forEach(column -> updateProperties.remove(PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + column));
            parquetBloomFilterColumns.forEach(column -> updateProperties.set(PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + column, "true"));
        }

        if (properties.containsKey(ORC_BLOOM_FILTER_COLUMNS_PROPERTY)) {
            checkFormatForProperty(getFileFormat(icebergTable).toIceberg(), FileFormat.ORC, ORC_BLOOM_FILTER_COLUMNS_PROPERTY);
            @SuppressWarnings("unchecked")
            List<String> orcBloomFilterColumns = (List<String>) properties.get(ORC_BLOOM_FILTER_COLUMNS_PROPERTY)
                    .orElseThrow(() -> new IllegalArgumentException("The orc_bloom_filter_columns property cannot be empty"));
            if (orcBloomFilterColumns.isEmpty()) {
                updateProperties.remove(ORC_BLOOM_FILTER_COLUMNS);
            }
            else {
                validateOrcBloomFilterColumns(getColumnMetadatas(SchemaParser.fromJson(table.getTableSchemaJson()), typeManager, table.getFormatVersion()), orcBloomFilterColumns);
                updateProperties.set(ORC_BLOOM_FILTER_COLUMNS, Joiner.on(",").join(orcBloomFilterColumns));
            }
        }

        IcebergFileFormat oldFileFormat = getFileFormat(icebergTable.properties());
        IcebergFileFormat newFileFormat = oldFileFormat;

        if (properties.containsKey(FILE_FORMAT_PROPERTY)) {
            newFileFormat = (IcebergFileFormat) properties.get(FILE_FORMAT_PROPERTY)
                    .orElseThrow(() -> new IllegalArgumentException("The format property cannot be empty"));
            updateProperties.defaultFormat(newFileFormat.toIceberg());
        }

        if (properties.containsKey(FORMAT_VERSION_PROPERTY)) {
            // UpdateProperties#commit will trigger any necessary metadata updates required for the new spec version
            int formatVersion = (int) properties.get(FORMAT_VERSION_PROPERTY)
                    .orElseThrow(() -> new IllegalArgumentException("The format_version property cannot be empty"));
            updateProperties.set(FORMAT_VERSION, Integer.toString(formatVersion));
        }

        Map<String, String> propertiesForCompression = calculateTableCompressionProperties(oldFileFormat, newFileFormat, icebergTable.properties(), properties.entrySet().stream()
                .filter(e -> e.getValue().isPresent())
                .collect(toImmutableMap(
                        Entry::getKey,
                        e -> e.getValue().get())));

        propertiesForCompression.forEach(updateProperties::set);

        if (properties.containsKey(MAX_COMMIT_RETRY)) {
            int maxCommitRetry = (int) properties.get(MAX_COMMIT_RETRY)
                    .orElseThrow(() -> new IllegalArgumentException("The max_commit_retry property cannot be empty"));
            updateProperties.set(COMMIT_NUM_RETRIES, Integer.toString(maxCommitRetry));
        }

        if (properties.containsKey(DELETE_AFTER_COMMIT_ENABLED)) {
            boolean deleteAfterCommitEnabled = (boolean) properties.get(DELETE_AFTER_COMMIT_ENABLED)
                    .orElseThrow(() -> new IllegalArgumentException("The %s property cannot be empty".formatted(DELETE_AFTER_COMMIT_ENABLED)));
            updateProperties.set(METADATA_DELETE_AFTER_COMMIT_ENABLED, Boolean.toString(deleteAfterCommitEnabled));
        }

        if (properties.containsKey(MAX_PREVIOUS_VERSIONS)) {
            int maxPreviousVersions = (int) properties.get(MAX_PREVIOUS_VERSIONS)
                    .orElseThrow(() -> new IllegalArgumentException("The %s property cannot be empty".formatted(MAX_PREVIOUS_VERSIONS)));
            updateProperties.set(METADATA_PREVIOUS_VERSIONS_MAX, Integer.toString(maxPreviousVersions));
        }

        if (properties.containsKey(OBJECT_STORE_LAYOUT_ENABLED_PROPERTY)) {
            boolean objectStoreEnabled = (boolean) properties.get(OBJECT_STORE_LAYOUT_ENABLED_PROPERTY)
                    .orElseThrow(() -> new IllegalArgumentException("The object_store_enabled property cannot be empty"));
            updateProperties.set(OBJECT_STORE_ENABLED, Boolean.toString(objectStoreEnabled));
        }

        if (properties.containsKey(DATA_LOCATION_PROPERTY)) {
            String dataLocation = (String) properties.get(DATA_LOCATION_PROPERTY)
                    .orElseThrow(() -> new IllegalArgumentException("The data_location property cannot be empty"));
            boolean objectStoreEnabled = (boolean) properties.getOrDefault(
                    OBJECT_STORE_LAYOUT_ENABLED_PROPERTY,
                    Optional.of(Boolean.parseBoolean(icebergTable.properties().get(OBJECT_STORE_ENABLED)))).orElseThrow();
            if (!objectStoreEnabled) {
                throw new TrinoException(INVALID_TABLE_PROPERTY, "Data location can only be set when object store layout is enabled");
            }
            updateProperties.set(WRITE_DATA_LOCATION, dataLocation);
        }

        if (properties.containsKey(MERGE_MODE_PROPERTY)) {
            RowLevelOperationMode writeMergeMode = RowLevelOperationMode.fromName((String) properties.get(MERGE_MODE_PROPERTY)
                    .orElseThrow(() -> new IllegalArgumentException("The merge_mode property cannot be empty")));
            updateProperties.set(MERGE_MODE, writeMergeMode.modeName());
        }

        try {
            updateProperties.commit();
        }
        catch (RuntimeException e) {
            throw new TrinoException(ICEBERG_COMMIT_ERROR, "Failed to set new property values", e);
        }

        if (properties.containsKey(PARTITIONING_PROPERTY)) {
            @SuppressWarnings("unchecked")
            List<String> partitionColumns = (List<String>) properties.get(PARTITIONING_PROPERTY)
                    .orElseThrow(() -> new IllegalArgumentException("The partitioning property cannot be empty"));
            updatePartitioning(icebergTable, transaction, partitionColumns);
        }

        if (properties.containsKey(SORTED_BY_PROPERTY)) {
            @SuppressWarnings("unchecked")
            List<String> sortColumns = (List<String>) properties.get(SORTED_BY_PROPERTY)
                    .orElseThrow(() -> new IllegalArgumentException("The sorted_by property cannot be empty"));
            ReplaceSortOrder replaceSortOrder = transaction.replaceSortOrder();
            parseSortFields(replaceSortOrder, sortColumns);
            try {
                replaceSortOrder.commit();
            }
            catch (RuntimeException e) {
                throw new TrinoException(ICEBERG_COMMIT_ERROR, "Failed to set the sorted_by property", e);
            }
        }

        commitTransaction(transaction, "set table properties");
    }

    public static Map<String, String> calculateTableCompressionProperties(IcebergFileFormat oldFileFormat, IcebergFileFormat newFileFormat, Map<String, String> existingProperties, Map<String, Object> inputProperties)
    {
        ImmutableMap.Builder<String, String> newCompressionProperties = ImmutableMap.builder();

        Optional<HiveCompressionCodec> oldCompressionCodec = getHiveCompressionCodec(oldFileFormat, existingProperties);
        Optional<HiveCompressionCodec> newCompressionCodec = IcebergTableProperties.getCompressionCodec(inputProperties);

        Optional<HiveCompressionCodec> compressionCodec = newCompressionCodec.or(() -> oldCompressionCodec);

        validateCompression(newFileFormat, compressionCodec);

        compressionCodec.ifPresent(hiveCompressionCodec ->
                newCompressionProperties.put(
                        getCompressionPropertyName(newFileFormat),
                        toCompressionCodecTableProperty(newFileFormat, hiveCompressionCodec)));

        return newCompressionProperties.buildOrThrow();
    }

    static String toCompressionCodecTableProperty(IcebergFileFormat fileFormat, HiveCompressionCodec compressionCodec)
    {
        return switch (fileFormat) {
            case PARQUET -> switch (compressionCodec) {
                case NONE -> "uncompressed";
                case SNAPPY -> "snappy";
                case LZ4 -> "lz4";
                case ZSTD -> "zstd";
                case GZIP -> "gzip";
            };
            case ORC -> switch (compressionCodec) {
                case NONE -> "none";
                case SNAPPY -> "snappy";
                case LZ4 -> "lz4";
                case ZSTD -> "zstd";
                case GZIP -> "zlib";
            };
            case AVRO -> switch (compressionCodec) {
                case NONE -> "uncompressed";
                case SNAPPY -> "snappy";
                case LZ4 -> throw new TrinoException(NOT_SUPPORTED, "Compression codec LZ4 not supported for Avro");
                case ZSTD -> "zstd";
                case GZIP -> "gzip";
            };
        };
    }

    private static void updatePartitioning(Table icebergTable, Transaction transaction, List<String> partitionColumns)
    {
        UpdatePartitionSpec updatePartitionSpec = transaction.updateSpec();
        Set<PartitionField> existingPartitionFields = ImmutableSet.copyOf(icebergTable.spec().fields());
        Schema schema = icebergTable.schema();
        if (partitionColumns.isEmpty()) {
            existingPartitionFields.stream()
                    .map(partitionField -> toIcebergTerm(schema, partitionField))
                    .forEach(updatePartitionSpec::removeField);
        }
        else {
            PartitionSpec partitionSpec = parsePartitionFields(schema, partitionColumns, getAllPartitionFields(icebergTable));
            Set<PartitionField> partitionFields = ImmutableSet.copyOf(partitionSpec.fields());

            // avoid using difference() here as hashcode() is not implemented for all transforms that a PartitionField may reference
            existingPartitionFields.stream()
                    .filter(existing -> partitionFields.stream().noneMatch(newField -> newField.equals(existing)))
                    .map(PartitionField::name)
                    .forEach(updatePartitionSpec::removeField);

            partitionFields.stream()
                    .filter(newField -> existingPartitionFields.stream().noneMatch(existing -> existing.equals(newField)))
                    .forEach(partitionField -> updatePartitionSpec.addField(partitionField.name(), toIcebergTerm(schema, partitionField)));
        }

        try {
            updatePartitionSpec.commit();
        }
        catch (RuntimeException e) {
            throw new TrinoException(ICEBERG_COMMIT_ERROR, "Failed to set new partitioning value", e);
        }
    }

    private static Term toIcebergTerm(Schema schema, PartitionField partitionField)
    {
        return Expressions.transform(schema.findColumnName(partitionField.sourceId()), partitionField.transform());
    }

    @Override
    public void createBranch(ConnectorSession session, ConnectorTableHandle tableHandle, String branch, Optional<String> fromBranch, SaveMode saveMode, Map<String, Object> properties)
    {
        if (saveMode == SaveMode.REPLACE) {
            throw new TrinoException(NOT_SUPPORTED, "The connector does not support replacing branches");
        }
        checkArgument(properties.isEmpty(), "This connector does not support creating branches with properties");

        IcebergTableHandle table = (IcebergTableHandle) tableHandle;
        BaseTable icebergTable = catalog.loadTable(session, table.getSchemaTableName());
        ManageSnapshots manageSnapshots = icebergTable.manageSnapshots();
        try {
            if (fromBranch.isPresent()) {
                SnapshotRef ref = icebergTable.refs().get(fromBranch.get());
                if (ref == null || !ref.isBranch()) {
                    throw new TrinoException(GENERIC_USER_ERROR, "Branch '%s' does not exist".formatted(fromBranch.get()));
                }
                manageSnapshots.createBranch(branch, ref.snapshotId());
            }
            else {
                manageSnapshots.createBranch(branch);
            }
            manageSnapshots.commit();
        }
        catch (Exception e) {
            throw new TrinoException(ICEBERG_COMMIT_ERROR, "Failed to create branch", e);
        }
    }

    @Override
    public void dropBranch(ConnectorSession session, ConnectorTableHandle tableHandle, String branch)
    {
        if (branch.equals("main")) {
            throw new TrinoException(NOT_SUPPORTED, "Cannot drop 'main' branch");
        }
        IcebergTableHandle table = (IcebergTableHandle) tableHandle;
        BaseTable icebergTable = catalog.loadTable(session, table.getSchemaTableName());
        try {
            icebergTable.manageSnapshots()
                    .removeBranch(branch)
                    .commit();
        }
        catch (Exception e) {
            throw new TrinoException(ICEBERG_COMMIT_ERROR, "Failed to drop branch", e);
        }
    }

    @Override
    public void fastForwardBranch(ConnectorSession session, ConnectorTableHandle tableHandle, String from, String to)
    {
        IcebergTableHandle table = (IcebergTableHandle) tableHandle;
        Table icebergTable = catalog.loadTable(session, table.getSchemaTableName());
        try {
            icebergTable.manageSnapshots()
                    .fastForwardBranch(from, to)
                    .commit();
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(GENERIC_USER_ERROR, "Branch '%s' is not an ancestor of '%s'".formatted(from, to), e);
        }
        catch (Exception e) {
            throw new TrinoException(ICEBERG_COMMIT_ERROR, "Failed to fast forward branch", e);
        }
    }

    @Override
    public Collection<String> listBranches(ConnectorSession session, SchemaTableName tableName)
    {
        Table icebergTable = catalog.loadTable(session, tableName);
        return icebergTable.refs().entrySet().stream()
                .filter(ref -> ref.getValue().isBranch())
                .map(Map.Entry::getKey)
                .collect(toImmutableList());
    }

    @Override
    public boolean branchExists(ConnectorSession session, SchemaTableName tableName, String branch)
    {
        SnapshotRef ref = catalog.loadTable(session, tableName).refs().get(branch);
        return ref != null && ref.isBranch();
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column, ColumnPosition position)
    {
        // Spark doesn't support adding a NOT NULL column to Iceberg tables
        // Also, Spark throws an exception when reading the table if we add such columns and execute a rollback procedure
        // because they keep returning the latest table definition even after the rollback https://github.com/apache/iceberg/issues/5591
        // Even when a table is empty, this connector doesn't support adding not null columns to avoid the above Spark failure
        if (!column.isNullable()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support adding not null columns");
        }
        Table icebergTable = catalog.loadTable(session, ((IcebergTableHandle) tableHandle).getSchemaTableName());
        checkDefaultValueCompatibility(formatVersion(icebergTable), column);
        // Start explicitly with highestFieldId + 2 to account for existing columns and the new one being
        // added - instead of relying on addColumn in iceberg library to assign Ids
        AtomicInteger nextFieldId = new AtomicInteger(icebergTable.schema().highestFieldId() + 2);
        try {
            UpdateSchema updateSchema = icebergTable.updateSchema();
            org.apache.iceberg.types.Type icebergType = toIcebergTypeForNewColumn(column.getType(), nextFieldId);

            // Handle default value if present
            Literal<?> defaultLiteral = column.getDefaultValue()
                    .map(defaultValue -> {
                        Object parsed = parseDefaultValue(defaultValue, column.getType(), icebergType);
                        return toIcebergLiteral(parsed, icebergType);
                    })
                    .orElse(null);

            updateSchema.addColumn(null, column.getName(), icebergType, column.getComment().orElse(null), defaultLiteral);
            switch (position) {
                case ColumnPosition.First _ -> updateSchema.moveFirst(column.getName());
                case ColumnPosition.After after -> updateSchema.moveAfter(column.getName(), after.columnName());
                case ColumnPosition.Last _ -> {}
            }
            updateSchema.commit();
        }
        catch (RuntimeException e) {
            throw new TrinoException(ICEBERG_COMMIT_ERROR, "Failed to add column: " + requireNonNullElse(e.getMessage(), e), e);
        }
    }

    @Override
    public void setDefaultValue(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column, String defaultValue)
    {
        IcebergTableHandle icebergTableHandle = (IcebergTableHandle) tableHandle;
        IcebergColumnHandle columnHandle = (IcebergColumnHandle) column;

        Table icebergTable = catalog.loadTable(session, icebergTableHandle.getSchemaTableName());
        checkDefaultValueCompatibility(formatVersion(icebergTable), true);

        NestedField field = icebergTable.schema().findField(columnHandle.getId());
        Object parsed = parseDefaultValue(defaultValue, columnHandle.getType(), field.type());
        Literal<?> defaultLiteral = toIcebergLiteral(parsed, field.type());

        try {
            icebergTable.updateSchema()
                    .updateColumnDefault(field.name(), defaultLiteral)
                    .commit();
        }
        catch (RuntimeException e) {
            throw new TrinoException(ICEBERG_COMMIT_ERROR, "Failed to set default value: " + requireNonNullElse(e.getMessage(), e), e);
        }
    }

    @Override
    public void dropDefaultValue(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column)
    {
        IcebergTableHandle icebergTableHandle = (IcebergTableHandle) tableHandle;
        IcebergColumnHandle columnHandle = (IcebergColumnHandle) column;

        Table icebergTable = catalog.loadTable(session, icebergTableHandle.getSchemaTableName());
        checkDefaultValueCompatibility(formatVersion(icebergTable), true);

        NestedField field = icebergTable.schema().findField(columnHandle.getId());
        try {
            icebergTable.updateSchema()
                    .updateColumnDefault(field.name(), null)
                    .commit();
        }
        catch (RuntimeException e) {
            throw new TrinoException(ICEBERG_COMMIT_ERROR, "Failed to drop default value: " + requireNonNullElse(e.getMessage(), e), e);
        }
    }

    @Override
    public void addField(ConnectorSession session, ConnectorTableHandle tableHandle, List<String> parentPath, String fieldName, io.trino.spi.type.Type type, boolean ignoreExisting)
    {
        Table icebergTable = catalog.loadTable(session, ((IcebergTableHandle) tableHandle).getSchemaTableName());
        NestedField parent = icebergTable.schema().caseInsensitiveFindField(String.join(".", parentPath));
        String caseSensitiveParentName = icebergTable.schema().findColumnName(parent.fieldId());

        NestedField field = icebergTable.schema().caseInsensitiveFindField(caseSensitiveParentName + "." + fieldName);
        if (field != null) {
            if (ignoreExisting) {
                return;
            }
            throw new TrinoException(COLUMN_ALREADY_EXISTS, "Field '%s' already exists".formatted(fieldName));
        }

        try {
            icebergTable.updateSchema()
                    .addColumn(caseSensitiveParentName, fieldName, toIcebergTypeForNewColumn(type, new AtomicInteger())) // Iceberg library assigns fresh id internally
                    .commit();
        }
        catch (RuntimeException e) {
            throw new TrinoException(ICEBERG_COMMIT_ERROR, "Failed to add field: " + requireNonNullElse(e.getMessage(), e), e);
        }
    }

    @Override
    public void dropColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column)
    {
        IcebergColumnHandle handle = (IcebergColumnHandle) column;
        dropField(session, tableHandle, handle.getName());
    }

    @Override
    public void dropField(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column, List<String> fieldPath)
    {
        IcebergColumnHandle handle = (IcebergColumnHandle) column;
        // Iceberg disallows ambiguous field names in a table. e.g. (a row(b int), "a.b" int)
        String name = String.join(".", ImmutableList.<String>builder().add(handle.getName()).addAll(fieldPath).build());
        dropField(session, tableHandle, name);
    }

    private void dropField(ConnectorSession session, ConnectorTableHandle tableHandle, String name)
    {
        Table icebergTable = catalog.loadTable(session, ((IcebergTableHandle) tableHandle).getSchemaTableName());
        long fieldId = icebergTable.schema().findField(name).fieldId();
        boolean isPartitionColumn = icebergTable.spec().fields().stream()
                .anyMatch(field -> field.sourceId() == fieldId);
        if (isPartitionColumn) {
            throw new TrinoException(NOT_SUPPORTED, "Cannot drop partition field: " + name);
        }
        boolean isSortColumn = icebergTable.sortOrder().fields().stream()
                .anyMatch(field -> field.sourceId() == fieldId);
        if (isSortColumn) {
            throw new TrinoException(NOT_SUPPORTED, "Cannot drop sort field: " + name);
        }
        int currentSpecId = icebergTable.spec().specId();
        boolean columnUsedInOlderPartitionSpecs = icebergTable.specs().entrySet().stream()
                .filter(spec -> spec.getValue().specId() != currentSpecId)
                .flatMap(spec -> spec.getValue().fields().stream())
                .anyMatch(field -> field.sourceId() == fieldId);
        if (columnUsedInOlderPartitionSpecs) {
            // After dropping a column which was used in older partition specs, insert/update/select fails on the table.
            // So restricting user to dropping that column. https://github.com/trinodb/trino/issues/15729
            throw new TrinoException(NOT_SUPPORTED, "Cannot drop column which is used by an old partition spec: " + name);
        }
        try {
            icebergTable.updateSchema()
                    .deleteColumn(name)
                    .commit();
        }
        catch (RuntimeException e) {
            throw new TrinoException(ICEBERG_COMMIT_ERROR, "Failed to drop column: " + requireNonNullElse(e.getMessage(), e), e);
        }
    }

    @Override
    public void renameColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle source, String target)
    {
        IcebergColumnHandle columnHandle = (IcebergColumnHandle) source;
        Table icebergTable = catalog.loadTable(session, ((IcebergTableHandle) tableHandle).getSchemaTableName());
        try {
            icebergTable.updateSchema()
                    .renameColumn(columnHandle.getName(), target)
                    .commit();
        }
        catch (RuntimeException e) {
            throw new TrinoException(ICEBERG_COMMIT_ERROR, "Failed to rename column: " + requireNonNullElse(e.getMessage(), e), e);
        }
    }

    @Override
    public void renameField(ConnectorSession session, ConnectorTableHandle tableHandle, List<String> fieldPath, String target)
    {
        Table icebergTable = catalog.loadTable(session, ((IcebergTableHandle) tableHandle).getSchemaTableName());
        NestedField field = icebergTable.schema().caseInsensitiveFindField(String.join(".", fieldPath));
        verify(field != null, "Field %s not found", String.join(".", fieldPath));

        String caseSensitiveFieldName = icebergTable.schema().findColumnName(field.fieldId());
        try {
            icebergTable.updateSchema()
                    .renameColumn(caseSensitiveFieldName, target)
                    .commit();
        }
        catch (RuntimeException e) {
            throw new TrinoException(ICEBERG_COMMIT_ERROR, "Failed to rename field: " + requireNonNullElse(e.getMessage(), e), e);
        }
    }

    @Override
    public void setColumnType(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle, io.trino.spi.type.Type type)
    {
        IcebergTableHandle table = (IcebergTableHandle) tableHandle;
        IcebergColumnHandle column = (IcebergColumnHandle) columnHandle;
        verify(column.isBaseColumn(), "Cannot change nested field types");

        Table icebergTable = catalog.loadTable(session, table.getSchemaTableName());
        Type sourceType = icebergTable.schema().findType(column.getName());
        AtomicInteger nextFieldId = new AtomicInteger(1);
        Type newType = toIcebergTypeForNewColumn(type, nextFieldId);
        try {
            UpdateSchema schemaUpdate = icebergTable.updateSchema();
            buildUpdateSchema(column.getName(), sourceType, newType, schemaUpdate);
            schemaUpdate.commit();
        }
        catch (RuntimeException e) {
            throw new TrinoException(ICEBERG_COMMIT_ERROR, "Failed to set column type: " + requireNonNullElse(e.getMessage(), e), e);
        }
    }

    private static void buildUpdateSchema(String name, Type sourceType, Type newType, UpdateSchema schemaUpdate)
    {
        if (sourceType.equals(newType)) {
            return;
        }
        if (sourceType.isPrimitiveType() && newType.isPrimitiveType()) {
            schemaUpdate.updateColumn(name, newType.asPrimitiveType());
            return;
        }

        switch (sourceType) {
            case StructType sourceStructType when newType instanceof StructType newStructType -> {
                updateFields(name, sourceStructType, newStructType, schemaUpdate);
                orderFields(name, newStructType, schemaUpdate);
            }
            case ListType sourceListType when newType instanceof ListType newListType -> {
                buildUpdateSchema(name + ".element", sourceListType.elementType(), newListType.elementType(), schemaUpdate);
            }
            case Types.MapType sourceMapType when newType instanceof Types.MapType newMapType -> {
                buildUpdateSchema(name + ".key", sourceMapType.keyType(), newMapType.keyType(), schemaUpdate);
                buildUpdateSchema(name + ".value", sourceMapType.valueType(), newMapType.valueType(), schemaUpdate);
            }
            default -> throw new IllegalArgumentException("Cannot change type from %s to %s".formatted(sourceType, newType));
        }
    }

    private static void updateFields(String name, NestedType sourceNestedType, NestedType newNestedType, UpdateSchema schemaUpdate)
    {
        List<String> fieldNames = Streams.concat(sourceNestedType.fields().stream(), newNestedType.fields().stream())
                .map(NestedField::name)
                .distinct()
                .collect(toImmutableList());

        for (String fieldName : fieldNames) {
            if (fieldExists(sourceNestedType, fieldName) && fieldExists(newNestedType, fieldName)) {
                buildUpdateSchema(name + "." + fieldName, sourceNestedType.fieldType(fieldName), newNestedType.fieldType(fieldName), schemaUpdate);
            }
            else if (fieldExists(newNestedType, fieldName)) {
                schemaUpdate.addColumn(name, fieldName, newNestedType.fieldType(fieldName));
            }
            else {
                schemaUpdate.deleteColumn(name + "." + fieldName);
            }
        }
    }

    private static void orderFields(String name, NestedType newNestedType, UpdateSchema schemaUpdate)
    {
        String currentName = null;
        for (NestedField field : newNestedType.fields()) {
            String path = name + "." + field.name();
            if (currentName == null) {
                schemaUpdate.moveFirst(path);
            }
            else {
                schemaUpdate.moveAfter(path, currentName);
            }
            currentName = path;
        }
    }

    private static boolean fieldExists(NestedType nestedType, String fieldName)
    {
        for (NestedField field : nestedType.fields()) {
            if (field.name().equals(fieldName)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void setFieldType(ConnectorSession session, ConnectorTableHandle tableHandle, List<String> fieldPath, io.trino.spi.type.Type type)
    {
        Table icebergTable = catalog.loadTable(session, ((IcebergTableHandle) tableHandle).getSchemaTableName());
        NestedField field = icebergTable.schema().caseInsensitiveFindField(String.join(".", fieldPath));
        String caseSensitiveFieldName = icebergTable.schema().findColumnName(field.fieldId());

        Type icebergType = toIcebergTypeForNewColumn(type, new AtomicInteger());
        UpdateSchema schemaUpdate = icebergTable.updateSchema();
        try {
            buildUpdateSchema(caseSensitiveFieldName, field.type(), icebergType, schemaUpdate);
            schemaUpdate.commit();
        }
        catch (RuntimeException e) {
            throw new TrinoException(ICEBERG_COMMIT_ERROR, "Failed to set field type: " + requireNonNullElse(e.getMessage(), e), e);
        }
    }

    @Override
    public void dropNotNullConstraint(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        IcebergTableHandle table = (IcebergTableHandle) tableHandle;
        IcebergColumnHandle column = (IcebergColumnHandle) columnHandle;

        Table icebergTable = catalog.loadTable(session, table.getSchemaTableName());
        verify(column.isBaseColumn(), "Cannot drop a not null constraint on nested fields");

        try {
            icebergTable.updateSchema()
                    .makeColumnOptional(column.getName())
                    .commit();
        }
        catch (RuntimeException e) {
            throw new TrinoException(ICEBERG_COMMIT_ERROR, "Failed to drop a not null constraint: " + requireNonNullElse(e.getMessage(), e), e);
        }
    }

    @Override
    public TableStatisticsMetadata getStatisticsCollectionMetadataForWrite(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean tableReplace)
    {
        if (!isCollectExtendedStatisticsOnWrite(session)) {
            return TableStatisticsMetadata.empty();
        }

        if (tableReplace) {
            return getStatisticsCollectionMetadata(tableMetadata, Optional.empty(), availableColumnNames -> {});
        }

        ConnectorTableHandle tableHandle = getTableHandle(session, tableMetadata.getTable(), Optional.empty(), Optional.empty());
        if (tableHandle == null) {
            // Assume new table (CTAS), collect NDV stats on all columns
            return getStatisticsCollectionMetadata(tableMetadata, Optional.empty(), availableColumnNames -> {});
        }
        IcebergTableHandle table = checkValidTableHandle(tableHandle);
        if (table.getSnapshotId().isEmpty()) {
            // Table has no data (empty, or wiped out). Collect NDV stats on all columns
            return getStatisticsCollectionMetadata(tableMetadata, Optional.empty(), availableColumnNames -> {});
        }

        Table icebergTable = catalog.loadTable(session, table.getSchemaTableName());
        long snapshotId = table.getSnapshotId().orElseThrow();
        Snapshot snapshot = icebergTable.snapshot(snapshotId);
        String totalRecords = snapshot.summary().get(TOTAL_RECORDS_PROP);
        if (totalRecords != null && Long.parseLong(totalRecords) == 0) {
            // Table has no data (empty, or wiped out). Collect NDV stats on all columns
            return getStatisticsCollectionMetadata(tableMetadata, Optional.empty(), availableColumnNames -> {});
        }

        Schema schema = SchemaParser.fromJson(table.getTableSchemaJson());
        List<IcebergColumnHandle> columns = getTopLevelColumns(schema, typeManager);
        Set<Integer> columnIds = columns.stream()
                .map(IcebergColumnHandle::getId)
                .collect(toImmutableSet());
        Map<Integer, Long> ndvs = readNdvs(icebergTable, snapshotId, columnIds);
        // Avoid collecting NDV stats on columns where we don't know the existing NDV count
        Set<String> columnsWithExtendedStatistics = columns.stream()
                .filter(column -> ndvs.containsKey(column.getId()))
                .map(IcebergColumnHandle::getName)
                .collect(toImmutableSet());
        return getStatisticsCollectionMetadata(tableMetadata, Optional.of(columnsWithExtendedStatistics), availableColumnNames -> {});
    }

    @Override
    public ConnectorAnalyzeMetadata getStatisticsCollectionMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, Map<String, Object> analyzeProperties)
    {
        IcebergTableHandle handle = checkValidTableHandle(tableHandle);

        checkArgument(handle.getTableType() == DATA, "Cannot analyze non-DATA table: %s", handle.getTableType());

        if (handle.getSnapshotId().isEmpty()) {
            // No snapshot, table is empty
            return new ConnectorAnalyzeMetadata(tableHandle, TableStatisticsMetadata.empty());
        }

        ConnectorTableMetadata tableMetadata = getTableMetadata(session, handle);
        Optional<Set<String>> analyzeColumnNames = getColumnNames(analyzeProperties)
                .map(columnNames -> {
                    // validate that proper column names are passed via `columns` analyze property
                    if (columnNames.isEmpty()) {
                        throw new TrinoException(INVALID_ANALYZE_PROPERTY, "Cannot specify empty list of columns for analysis");
                    }
                    return columnNames;
                });

        return new ConnectorAnalyzeMetadata(
                handle.forAnalyze(),
                getStatisticsCollectionMetadata(
                        tableMetadata,
                        analyzeColumnNames,
                        availableColumnNames -> {
                            throw new TrinoException(
                                    INVALID_ANALYZE_PROPERTY,
                                    format("Invalid columns specified for analysis: %s", Sets.difference(analyzeColumnNames.orElseThrow(), availableColumnNames)));
                        }));
    }

    private TableStatisticsMetadata getStatisticsCollectionMetadata(
            ConnectorTableMetadata tableMetadata,
            Optional<Set<String>> selectedColumnNames,
            Consumer<Set<String>> unsatisfiableSelectedColumnsHandler)
    {
        Set<String> allScalarColumnNames = tableMetadata.getColumns().stream()
                .filter(column -> !column.isHidden())
                .filter(column -> {
                    io.trino.spi.type.Type type = column.getType();
                    return !(type instanceof MapType || type instanceof ArrayType || type instanceof RowType); // is scalar type
                })
                .map(ColumnMetadata::getName)
                .collect(toImmutableSet());

        selectedColumnNames.ifPresent(columnNames -> {
            if (!allScalarColumnNames.containsAll(columnNames)) {
                unsatisfiableSelectedColumnsHandler.accept(allScalarColumnNames);
            }
        });

        Set<ColumnStatisticMetadata> columnStatistics = tableMetadata.getColumns().stream()
                .filter(columnMetadata -> allScalarColumnNames.contains(columnMetadata.getName()))
                .filter(selectedColumnNames
                        .map(columnNames -> (Predicate<ColumnMetadata>) columnMetadata -> columnNames.contains(columnMetadata.getName()))
                        .orElse(columnMetadata -> true))
                .map(column -> new ColumnStatisticMetadata(column.getName(), NUMBER_OF_DISTINCT_VALUES_NAME, NUMBER_OF_DISTINCT_VALUES_FUNCTION))
                .collect(toImmutableSet());

        return new TableStatisticsMetadata(columnStatistics, ImmutableSet.of(), ImmutableList.of());
    }

    @Override
    public ConnectorTableHandle beginStatisticsCollection(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle handle = (IcebergTableHandle) tableHandle;
        Table icebergTable = catalog.loadTable(session, handle.getSchemaTableName());
        if (isS3Tables(icebergTable.location())) {
            throw new TrinoException(NOT_SUPPORTED, "S3 Tables do not support analyze");
        }
        beginTransaction(icebergTable);
        return handle;
    }

    @Override
    public void finishStatisticsCollection(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<ComputedStatistics> computedStatistics)
    {
        IcebergTableHandle handle = (IcebergTableHandle) tableHandle;
        Table table = transaction.table();
        if (handle.getSnapshotId().isEmpty()) {
            // No snapshot, table is empty
            verify(
                    computedStatistics.size() == 1,
                    "The computedStatistics size must be 1: %s",
                    computedStatistics);
            ComputedStatistics statistics = getOnlyElement(computedStatistics);
            verify(statistics.getGroupingColumns().isEmpty() &&
                            statistics.getGroupingValues().isEmpty() &&
                            statistics.getColumnStatistics().isEmpty() &&
                            statistics.getTableStatistics().isEmpty(),
                    "Unexpected non-empty statistics that cannot be attached to a snapshot because none exists: %s",
                    computedStatistics);

            commitTransaction(transaction, "statistics collection");
            transaction = null;
            return;
        }
        long snapshotId = handle.getSnapshotId().orElseThrow();

        CollectedStatistics collectedStatistics = processComputedTableStatistics(table, computedStatistics);
        StatisticsFile statisticsFile = tableStatisticsWriter.writeStatisticsFile(
                session,
                table,
                snapshotId,
                REPLACE,
                collectedStatistics);
        transaction.updateStatistics()
                .setStatistics(statisticsFile)
                .commit();

        partitionStatisticsWriter.writePartitionStats(session, table, snapshotId).ifPresent(partitionStatisticsFile -> {
            transaction.updatePartitionStatistics()
                    .setPartitionStatistics(partitionStatisticsFile)
                    .commit();
        });

        commitTransaction(transaction, "statistics collection");
        transaction = null;
    }

    @Override
    public Optional<ConnectorTableHandle> applyDelete(ConnectorSession session, ConnectorTableHandle handle)
    {
        IcebergTableHandle table = (IcebergTableHandle) handle;
        TupleDomain<IcebergColumnHandle> medataColumnPredicate = table.getEnforcedPredicate().filter((column, domain) -> isMetadataColumnId(column.getId()));
        if (!medataColumnPredicate.isAll()) {
            return Optional.empty();
        }
        return Optional.of(handle);
    }

    @Override
    public RowChangeParadigm getRowChangeParadigm(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return DELETE_ROW_AND_INSERT_ROW;
    }

    @Override
    public ColumnHandle getMergeRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        StructType type = StructType.of(ImmutableList.<NestedField>builder()
                .add(MetadataColumns.FILE_PATH)
                .add(MetadataColumns.ROW_POSITION)
                .add(NestedField.required(TRINO_MERGE_PARTITION_SPEC_ID, "partition_spec_id", IntegerType.get()))
                .add(NestedField.required(TRINO_MERGE_PARTITION_DATA, "partition_data", StringType.get()))
                .add(NestedField.optional(TRINO_MERGE_SOURCE_ROW_ID, "source_row_id", LongType.get()))
                .build());

        NestedField field = NestedField.required(TRINO_MERGE_ROW_ID, TRINO_MERGE_ROW_ID_NAME, type);
        return getColumnHandle(field, typeManager);
    }

    @Override
    public Optional<ConnectorPartitioningHandle> getUpdateLayout(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        Optional<ConnectorTableLayout> insertLayout = getInsertLayout(session, tableHandle);
        if (rowLevelOperationMode(catalog.loadTable(session, ((IcebergTableHandle) tableHandle).getSchemaTableName())) == COPY_ON_WRITE) {
            if (insertLayout.isEmpty() || insertLayout.get().getPartitioning().isEmpty()) {
                return Optional.of(new IcebergPartitioningHandle(true, List.of()));
            }
        }
        return insertLayout
                .flatMap(ConnectorTableLayout::getPartitioning)
                .map(IcebergPartitioningHandle.class::cast)
                .map(IcebergPartitioningHandle::forUpdate);
    }

    @Override
    public ConnectorMergeTableHandle beginMerge(ConnectorSession session, ConnectorTableHandle tableHandle, Map<Integer, Collection<ColumnHandle>> updateCaseColumns, RetryMode retryMode)
    {
        IcebergTableHandle table = (IcebergTableHandle) tableHandle;
        verifyTableVersionForUpdate(table);

        Table icebergTable = catalog.loadTable(session, table.getSchemaTableName());
        Optional<String> branch = table.getBranch();

        branch.ifPresentOrElse(
                name -> checkBranch(icebergTable, name),
                () -> validateNotModifyingOldSnapshot(table, icebergTable));

        beginTransaction(icebergTable);

        List<PositionDeleteFiles> previousDeleteFiles = loadPreviousDeleteFiles(icebergTable, branch);
        IcebergWritableTableHandle insertHandle = newWritableTableHandle(table.getSchemaTableName(), icebergTable, table.getTableSchemaJson(), branch, previousDeleteFiles);
        return new IcebergMergeTableHandle(table, insertHandle);
    }

    private List<PositionDeleteFiles> loadPreviousDeleteFiles(Table icebergTable, Optional<String> branch)
    {
        int formatVersion = formatVersion(icebergTable);
        validateFormatVersion(formatVersion, maxFormatVersion);
        RowLevelOperationMode operationMode = rowLevelOperationMode(icebergTable);
        if (operationMode == MERGE_ON_READ) {
            return ImmutableList.of();
        }

        TableScan tableScan = icebergTable.newScan();
        if (branch.isPresent()) {
            tableScan = tableScan.useRef(branch.get());
        }

        ImmutableList.Builder<PositionDeleteFiles> rewritableDeletes = ImmutableList.builder();
        try (CloseableIterable<FileScanTask> iterator = tableScan.planFiles()) {
            for (FileScanTask task : iterator) {
                DataFile file = task.file();
                rewritableDeletes.add(new PositionDeleteFiles(
                        file.location(),
                        file.recordCount(),
                        file.dataSequenceNumber(),
                        file.firstRowId(),
                        file.specId(),
                        task.deletes().stream()
                                .map(deleteFile -> ContentFileParser.toJson(deleteFile, task.spec()))
                                .collect(toImmutableList()),
                        task.deletes().stream().collect(toImmutableMap(DeleteFile::location, DeleteFile::dataSequenceNumber))));

                fileMetrics.put(file.location(), new org.apache.iceberg.Metrics(
                        file.recordCount(),
                        file.columnSizes(),
                        file.valueCounts(),
                        file.nullValueCounts(),
                        file.nanValueCounts(),
                        file.lowerBounds(),
                        file.upperBounds()));
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return rewritableDeletes.build();
    }

    @Override
    public void finishMerge(ConnectorSession session, ConnectorMergeTableHandle mergeTableHandle, List<ConnectorTableHandle> sourceTableHandles, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        IcebergMergeTableHandle mergeHandle = (IcebergMergeTableHandle) mergeTableHandle;
        IcebergTableHandle handle = mergeHandle.getTableHandle();
        Optional<String> branch = mergeHandle.getInsertTableHandle().branch();
        RowLevelOperationMode operationMode = mergeHandle.getInsertTableHandle().operationMode();
        finishWrite(session, handle, branch, fragments, operationMode);
    }

    private static void verifyTableVersionForUpdate(IcebergTableHandle table)
    {
        int formatVersion = table.getFormatVersion();
        if (formatVersion < 2) {
            throw new TrinoException(NOT_SUPPORTED, "Iceberg table updates require at least format version 2");
        }
    }

    private static void validateNotModifyingOldSnapshot(IcebergTableHandle table, Table icebergTable)
    {
        if (table.getSnapshotId().isPresent() && (table.getSnapshotId().getAsLong() != icebergTable.currentSnapshot().snapshotId())) {
            throw new TrinoException(NOT_SUPPORTED, "Modifying old snapshot is not supported in Iceberg");
        }
    }

    private void finishWrite(ConnectorSession session, IcebergTableHandle table, Optional<String> branch, Collection<Slice> fragments, RowLevelOperationMode operationMode)
    {
        Table icebergTable = transaction.table();

        List<CommitTaskData> commitTasks = fragments.stream()
                .map(Slice::getInput)
                .map(commitTaskCodec::fromJson)
                .collect(toImmutableList());

        if (commitTasks.isEmpty()) {
            // Avoid recording "empty" write operation
            transaction = null;
            return;
        }

        Schema schema = SchemaParser.fromJson(table.getTableSchemaJson());

        UpdateSnapshot rowDelta = switch (operationMode) {
            case MERGE_ON_READ -> new MergeOnRead(transaction.newRowDelta());
            case COPY_ON_WRITE -> new CopyOnWrite(transaction.newOverwrite());
        };
        branch.ifPresent(rowDelta::toBranch);
        OptionalLong baseSnapshotId = table.getSnapshotId();
        if (baseSnapshotId.isPresent()) {
            rowDelta.validateFromSnapshot(icebergTable.snapshot(baseSnapshotId.getAsLong()).snapshotId());
        }
        TupleDomain<IcebergColumnHandle> dataColumnPredicate = table.getEnforcedPredicate().filter((column, domain) -> !isMetadataColumnId(column.getId()));
        TupleDomain<IcebergColumnHandle> effectivePredicate = dataColumnPredicate.intersect(table.getUnenforcedPredicate());
        if (isFileBasedConflictDetectionEnabled(session)) {
            effectivePredicate = effectivePredicate.intersect(extractTupleDomainsFromCommitTasks(table, icebergTable, commitTasks, typeManager));
        }

        effectivePredicate = effectivePredicate.filter((_, domain) -> isConvertibleToIcebergExpression(domain));

        if (!effectivePredicate.isAll()) {
            rowDelta.conflictDetectionFilter(toIcebergExpression(effectivePredicate));
        }
        IsolationLevel isolationLevel = IsolationLevel.fromName(icebergTable.properties().getOrDefault(DELETE_ISOLATION_LEVEL, DELETE_ISOLATION_LEVEL_DEFAULT));
        if (isolationLevel == IsolationLevel.SERIALIZABLE) {
            rowDelta.validateNoConflicting();
        }

        // Ensure a row that is updated by this commit was not deleted by a separate commit
        rowDelta.validateNoConflictingDeleteFiles();
        rowDelta.scanManifestsWith(icebergScanExecutor);

        List<CommitTaskData> dataTasks = new ArrayList<>();
        List<CommitTaskData> deleteTasks = new ArrayList<>();

        for (CommitTaskData task : commitTasks) {
            switch (task.content()) {
                case DATA -> dataTasks.add(task);
                case POSITION_DELETES -> deleteTasks.add(task);
                case EQUALITY_DELETES -> throw new UnsupportedOperationException("Unsupported task content: " + task.content());
            }
        }

        for (CommitTaskData task : dataTasks) {
            PartitionSpec partitionSpec = PartitionSpecParser.fromJson(schema, task.partitionSpecJson());
            Type[] partitionColumnTypes = partitionSpec.fields().stream()
                    .map(field -> field.transform().getResultType(schema.findType(field.sourceId())))
                    .toArray(Type[]::new);

            Optional<PartitionData> partitionData = Optional.empty();
            if (!partitionSpec.fields().isEmpty()) {
                String partitionDataJson = task.partitionDataJson()
                        .orElseThrow(() -> new VerifyException("No partition data for partitioned table"));
                partitionData = Optional.of(PartitionData.fromJson(partitionDataJson, partitionColumnTypes));
            }

            Optional<DataFile> rewrittenDataFile = Optional.empty();
            if (task.referencedDataFile().isPresent()) {
                checkArgument(operationMode == COPY_ON_WRITE, "Referenced data file is only supported in COPY_ON_WRITE mode");
                String referencedDataFile = task.referencedDataFile().get();
                checkArgument(fileMetrics.containsKey(referencedDataFile), "Unable to find metrics for referenced data file: %s", referencedDataFile);
                DataFiles.Builder rewrittenDataFileBuilder = DataFiles.builder(partitionSpec)
                        .withInputFile(icebergTable.io().newInputFile(referencedDataFile))
                        .withMetrics(fileMetrics.get(referencedDataFile))
                        .withPath(referencedDataFile);
                partitionData.ifPresent(rewrittenDataFileBuilder::withPartition);
                rewrittenDataFile = Optional.of(rewrittenDataFileBuilder.build());
            }
            if (task.path().isEmpty()) {
                rowDelta.addRows(Optional.empty(), rewrittenDataFile);
                continue;
            }
            Map<Integer, SortOrder> sortOrders = icebergTable.sortOrders();
            DataFiles.Builder builder = DataFiles.builder(partitionSpec)
                    .withPath(task.path())
                    .withFormat(task.fileFormat().toIceberg())
                    .withFileSizeInBytes(task.fileSizeInBytes())
                    .withMetrics(task.metrics().metrics())
                    .withSortOrder(sortOrders.get(task.sortOrderId()));
            partitionData.ifPresent(builder::withPartition);
            task.fileSplitOffsets().ifPresent(builder::withSplitOffsets);

            if (!icebergTable.spec().fields().isEmpty()) {
                String partitionDataJson = task.partitionDataJson()
                        .orElseThrow(() -> new VerifyException("No partition data for partitioned table"));
                builder.withPartition(PartitionData.fromJson(partitionDataJson, partitionColumnTypes));
            }

            rowDelta.addRows(Optional.of(builder.build()), rewrittenDataFile);
        }

        if (deleteTasks.isEmpty()) {
            commitUpdateAndTransaction(rowDelta.unwrap(), session, transaction, "write");
            return;
        }

        if (table.getFormatVersion() < 2) {
            throw new TrinoException(ICEBERG_BAD_DATA, "Position delete files are not supported for Iceberg format version < 2");
        }

        rowDelta.validateDataFilesExist(deleteTasks.stream()
                .map(CommitTaskData::referencedDataFile)
                .flatMap(Optional::stream)
                .toList());

        if (table.getFormatVersion() == 2) {
            for (CommitTaskData task : deleteTasks) {
                PartitionSpec partitionSpec = PartitionSpecParser.fromJson(schema, task.partitionSpecJson());
                FileMetadata.Builder deleteBuilder = FileMetadata.deleteFileBuilder(partitionSpec)
                        .withPath(task.path())
                        .withFormat(task.fileFormat().toIceberg())
                        .ofPositionDeletes()
                        .withFileSizeInBytes(task.fileSizeInBytes())
                        .withMetrics(task.metrics().metrics());
                task.fileSplitOffsets().ifPresent(deleteBuilder::withSplitOffsets);
                toPartitionData(partitionSpec, schema, task.partitionDataJson()).ifPresent(deleteBuilder::withPartition);
                rowDelta.addDeletes(deleteBuilder.build());
            }
            commitUpdateAndTransaction(rowDelta.unwrap(), session, transaction, "write");
            return;
        }

        // v3 delete: deletion vector for updated files are merged with any existing delection vectors or legacy position delete files.
        List<DeletionVectorInfo> deletionVectorInfos = deleteTasks.stream()
                .map(task -> {
                    PartitionSpec partitionSpec = PartitionSpecParser.fromJson(schema, task.partitionSpecJson());
                    return new DeletionVectorInfo(
                            task.referencedDataFile().orElseThrow(() -> new VerifyException("v3 POSITION_DELETES task missing referencedDataFile")),
                            task.serializedDeletionVector()
                                    .map(Slices::wrappedBuffer)
                                    .orElseThrow(() -> new VerifyException("v3 POSITION_DELETES task missing serializedDeletionVector")),
                            partitionSpec,
                            toPartitionData(partitionSpec, schema, task.partitionDataJson()));
                })
                .toList();

        verify(operationMode == MERGE_ON_READ, "v3 POSITION_DELETES are only supported in MERGE_ON_READ mode");
        deletionVectorWriter.writeDeletionVectors(session, icebergTable, table, deletionVectorInfos, (RowDelta) rowDelta.unwrap());

        commitUpdateAndTransaction(rowDelta.unwrap(), session, transaction, "write");
    }

    private static Optional<PartitionData> toPartitionData(PartitionSpec partitionSpec, Schema schema, Optional<String> partitionDataJson)
    {
        if (partitionSpec.fields().isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(PartitionData.fromJson(
                partitionDataJson.orElseThrow(() -> new VerifyException("No partition data for partitioned table")),
                partitionSpec.fields().stream()
                        .map(field -> field.transform().getResultType(schema.findType(field.sourceId())))
                        .toArray(Type[]::new)));
    }

    static TupleDomain<IcebergColumnHandle> extractTupleDomainsFromCommitTasks(IcebergTableHandle table, Table icebergTable, List<CommitTaskData> commitTasks, TypeManager typeManager)
    {
        Set<IcebergColumnHandle> partitionColumns = new HashSet<>(getProjectedColumns(icebergTable.schema(), typeManager, identityPartitionColumnsInAllSpecs(icebergTable)));
        PartitionSpec partitionSpec = icebergTable.spec();
        Type[] partitionColumnTypes = partitionSpec.fields().stream()
                .map(field -> field.transform().getResultType(icebergTable.schema().findType(field.sourceId())))
                .toArray(Type[]::new);
        Schema schema = SchemaParser.fromJson(table.getTableSchemaJson());
        Map<IcebergColumnHandle, List<Domain>> domainsFromTasks = new HashMap<>();
        for (CommitTaskData commitTask : commitTasks) {
            PartitionSpec taskPartitionSpec = PartitionSpecParser.fromJson(schema, commitTask.partitionSpecJson());
            if (commitTask.partitionDataJson().isEmpty() || taskPartitionSpec.isUnpartitioned() || !taskPartitionSpec.equals(partitionSpec)) {
                // We should not produce any specific domains if there are no partitions or current partitions does not match task partitions for any of tasks
                // As each partition value narrows down conflict scope we should produce values from all commit tasks or not at all, to avoid partial information
                return TupleDomain.all();
            }

            PartitionData partitionData = PartitionData.fromJson(commitTask.partitionDataJson().get(), partitionColumnTypes);
            Map<Integer, Optional<String>> partitionKeys = getPartitionKeys(partitionData, partitionSpec);
            Map<ColumnHandle, NullableValue> partitionValues = getPartitionValues(partitionColumns, partitionKeys);

            for (Entry<ColumnHandle, NullableValue> entry : partitionValues.entrySet()) {
                IcebergColumnHandle columnHandle = (IcebergColumnHandle) entry.getKey();
                NullableValue value = entry.getValue();
                Domain newDomain = value.isNull() ? Domain.onlyNull(columnHandle.getType()) : Domain.singleValue(columnHandle.getType(), value.getValue());
                domainsFromTasks.computeIfAbsent(columnHandle, _ -> new ArrayList<>()).add(newDomain);
            }
        }
        return withColumnDomains(domainsFromTasks.entrySet().stream()
                .collect(toImmutableMap(
                        Entry::getKey,
                        entry -> Domain.union(entry.getValue()))));
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName viewName, ConnectorViewDefinition definition, Map<String, Object> viewProperties, boolean replace)
    {
        checkArgument(viewProperties.isEmpty(), "This connector does not support creating views with properties");
        catalog.createView(session, viewName, definition, replace);
    }

    @Override
    public void renameView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        catalog.renameView(session, source, target);
    }

    @Override
    public void setViewAuthorization(ConnectorSession session, SchemaTableName viewName, TrinoPrincipal principal)
    {
        catalog.setViewPrincipal(session, viewName, principal);
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        catalog.dropView(session, viewName);
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> schemaName)
    {
        return catalog.listViews(session, schemaName);
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, Optional<String> schemaName)
    {
        return catalog.getViews(session, schemaName);
    }

    @Override
    public boolean isView(ConnectorSession session, SchemaTableName viewName)
    {
        try {
            return catalog.getView(session, viewName).isPresent();
        }
        catch (TrinoException e) {
            if (e.getErrorCode() == ICEBERG_UNSUPPORTED_VIEW_DIALECT.toErrorCode()) {
                return true;
            }
            throw e;
        }
    }

    @Override
    public Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName viewName)
    {
        return catalog.getView(session, viewName);
    }

    @Override
    public OptionalLong executeDelete(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle handle = (IcebergTableHandle) tableHandle;

        Table icebergTable = catalog.loadTable(session, handle.getSchemaTableName());

        DeleteFiles deleteFiles = icebergTable.newDelete()
                .deleteFromRowFilter(toIcebergExpression(handle.getEnforcedPredicate()))
                .scanManifestsWith(icebergScanExecutor);

        handle.getBranch().ifPresent(branch -> {
            checkBranch(icebergTable, branch);
            deleteFiles.toBranch(branch);
        });

        commitUpdate(deleteFiles, session, "delete");

        Map<String, String> summary = icebergTable.currentSnapshot().summary();
        String deletedRowsStr = summary.get(DELETED_RECORDS_PROP);
        if (deletedRowsStr == null) {
            // TODO Iceberg should guarantee this is always present (https://github.com/apache/iceberg/issues/4647)
            return OptionalLong.empty();
        }
        long deletedRecords = Long.parseLong(deletedRowsStr);
        long removedPositionDeletes = Long.parseLong(summary.getOrDefault(REMOVED_POS_DELETES_PROP, "0"));
        long removedEqualityDeletes = Long.parseLong(summary.getOrDefault(REMOVED_EQ_DELETES_PROP, "0"));
        return OptionalLong.of(deletedRecords - removedPositionDeletes - removedEqualityDeletes);
    }

    @Override
    public void truncateTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle table = checkValidTableHandle(tableHandle);
        Table icebergTable = catalog.loadTable(session, table.getSchemaTableName());
        beginTransaction(icebergTable);
        transaction.newDelete()
                .deleteFromRowFilter(alwaysTrue())
                .scanManifestsWith(icebergScanExecutor)
                .commit();
        if (icebergTable.currentSnapshot() != null) {
            transaction.updatePartitionStatistics()
                    .removePartitionStatistics(icebergTable.currentSnapshot().snapshotId())
                    .commit();
        }
        commitTransaction(transaction, "truncate");
    }

    public void rollback()
    {
        // TODO: cleanup open transaction
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(ConnectorSession session, ConnectorTableHandle handle, long limit)
    {
        IcebergTableHandle table = (IcebergTableHandle) handle;

        if (table.getLimit().isPresent() && table.getLimit().getAsLong() <= limit) {
            return Optional.empty();
        }
        if (!table.getUnenforcedPredicate().isAll()) {
            return Optional.empty();
        }

        table = new IcebergTableHandle(
                table.getSchemaName(),
                table.getTableName(),
                table.getTableType(),
                table.getSnapshotId(),
                table.getTableSchemaJson(),
                table.getSpecId(),
                table.getPartitionSpecJsons(),
                table.getFormatVersion(),
                table.getUnenforcedPredicate(), // known to be ALL
                table.getEnforcedPredicate(),
                OptionalLong.of(limit),
                table.preferSmallInitialReads(),
                table.getProjectedColumns(),
                table.getNameMappingJson(),
                table.getTableLocation(),
                table.getStorageProperties(),
                table.getTablePartitioning(),
                table.getBranch(),
                table.isRecordScannedFiles(),
                table.getMaxScannedFileSize(),
                table.isForceReadingAllFiles(),
                table.getConstraintColumns(),
                table.getForAnalyze());

        return Optional.of(new LimitApplicationResult<>(table, false, false));
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle handle, Constraint constraint)
    {
        IcebergTableHandle table = (IcebergTableHandle) handle;
        TupleDomain<IcebergColumnHandle> predicate;
        ConnectorExpression connectorExpression;
        // TODO when using iceberg.time-zone with value other than UTC, then UtcConstraintExtractor does not behave correctly as
        //  UtcConstraintExtractor assumes all values of TIMESTAMP WITH TIME ZONE type are represented using the UTC time zone.
        //  Make UtcConstraintExtractor to work with other time zones as well, so we can push down constraints for other time zones.
        //  https://starburstdata.atlassian.net/browse/SEP-17396
        if (dateTimeZone.equals(UTC)) {
            UtcConstraintExtractor.ExtractionResult extractionResult = UtcConstraintExtractor.extractTupleDomain(constraint);
            predicate = extractionResult.tupleDomain()
                    .transformKeys(IcebergColumnHandle.class::cast);
            connectorExpression = extractionResult.remainingExpression();
        }
        else {
            predicate = constraint.getSummary()
                    .transformKeys(IcebergColumnHandle.class::cast);
            connectorExpression = constraint.getExpression();
        }
        if (predicate.isAll() && constraint.getPredicateColumns().isEmpty()) {
            return Optional.empty();
        }
        if (table.getLimit().isPresent()) {
            // TODO we probably can allow predicate pushdown after we accepted limit. Currently, this is theoretical because we don't enforce limit, so
            //  LimitNode remains above TableScan, and there is no "push filter through limit" optimization.
            return Optional.empty();
        }

        TupleDomain<IcebergColumnHandle> newEnforcedConstraint;
        TupleDomain<IcebergColumnHandle> newUnenforcedConstraint;
        TupleDomain<IcebergColumnHandle> remainingConstraint;
        if (predicate.isNone()) {
            // Engine does not pass none Constraint.summary. It can become none when combined with the expression and connector's domain knowledge.
            newEnforcedConstraint = TupleDomain.none();
            newUnenforcedConstraint = TupleDomain.all();
            remainingConstraint = TupleDomain.all();
        }
        else {
            Table icebergTable = catalog.loadTable(session, table.getSchemaTableName());

            Set<Integer> partitionSpecIds = getPartitionSpecIdsFromSnapshot(icebergTable, table.getSnapshotId());

            Map<IcebergColumnHandle, Domain> unsupported = new LinkedHashMap<>();
            Map<IcebergColumnHandle, Domain> newEnforced = new LinkedHashMap<>();
            Map<IcebergColumnHandle, Domain> newUnenforced = new LinkedHashMap<>();
            Map<IcebergColumnHandle, Domain> domains = predicate.getDomains().orElseThrow(() -> new VerifyException("No domains"));
            domains.forEach((columnHandle, domain) -> {
                if (!isConvertibleToIcebergExpression(domain)) {
                    unsupported.put(columnHandle, domain);
                }
                else if (canEnforceColumnConstraintInSpecs(typeManager.getTypeOperators(), icebergTable, partitionSpecIds, columnHandle, domain)) {
                    newEnforced.put(columnHandle, domain);
                }
                else if (isMetadataColumnId(columnHandle.getId())) {
                    if (columnHandle.isPartitionColumn() || columnHandle.isPathColumn() || columnHandle.isFileModifiedTimeColumn()) {
                        newEnforced.put(columnHandle, domain);
                    }
                    else {
                        unsupported.put(columnHandle, domain);
                    }
                }
                else {
                    newUnenforced.put(columnHandle, domain);
                }
            });

            newEnforcedConstraint = TupleDomain.withColumnDomains(newEnforced).intersect(table.getEnforcedPredicate());
            newUnenforcedConstraint = TupleDomain.withColumnDomains(newUnenforced).intersect(table.getUnenforcedPredicate());
            remainingConstraint = TupleDomain.withColumnDomains(newUnenforced).intersect(TupleDomain.withColumnDomains(unsupported));
        }

        Set<IcebergColumnHandle> newConstraintColumns = Streams.concat(
                        table.getConstraintColumns().stream(),
                        constraint.getPredicateColumns().orElseGet(ImmutableSet::of).stream()
                                .map(columnHandle -> (IcebergColumnHandle) columnHandle))
                .collect(toImmutableSet());

        if (newEnforcedConstraint.equals(table.getEnforcedPredicate())
                && newUnenforcedConstraint.equals(table.getUnenforcedPredicate())
                && newConstraintColumns.equals(table.getConstraintColumns())) {
            return Optional.empty();
        }

        return Optional.of(new ConstraintApplicationResult<>(
                new IcebergTableHandle(
                        table.getSchemaName(),
                        table.getTableName(),
                        table.getTableType(),
                        table.getSnapshotId(),
                        table.getTableSchemaJson(),
                        table.getSpecId(),
                        table.getPartitionSpecJsons(),
                        table.getFormatVersion(),
                        newUnenforcedConstraint,
                        newEnforcedConstraint,
                        table.getLimit(),
                        table.preferSmallInitialReads(),
                        table.getProjectedColumns(),
                        table.getNameMappingJson(),
                        table.getTableLocation(),
                        table.getStorageProperties(),
                        table.getTablePartitioning(),
                        table.getBranch(),
                        table.isRecordScannedFiles(),
                        table.getMaxScannedFileSize(),
                        table.isForceReadingAllFiles(),
                        newConstraintColumns,
                        table.getForAnalyze()),
                remainingConstraint.transformKeys(ColumnHandle.class::cast),
                connectorExpression,
                false));
    }

    private static Set<Integer> getPartitionSpecIdsFromSnapshot(Table icebergTable, OptionalLong snapshotId)
    {
        Map<Integer, PartitionSpec> specs = icebergTable.specs();
        // If there aren't multiple partition specs,
        // then we don't need to read manifest list to find the specs that belong to given snapshot
        if (specs.size() <= 1) {
            return specs.keySet();
        }
        if (snapshotId.isEmpty()) {
            // No snapshot, so no data. This case doesn't matter.
            return specs.keySet();
        }

        Snapshot snapshot = icebergTable.snapshot(snapshotId.getAsLong());
        // Since we're primarily concerned about predicate pushdown on partitioning
        // of the data files, there's no need to consider delete manifests
        return loadDataManifestsFromSnapshot(icebergTable, snapshot).stream()
                .map(ManifestFile::partitionSpecId)
                .collect(toImmutableSet());
    }

    private static List<ManifestFile> loadDataManifestsFromSnapshot(Table icebergTable, @Nullable Snapshot snapshot)
    {
        if (snapshot == null) {
            return ImmutableList.of();
        }
        try {
            return snapshot.dataManifests(icebergTable.io());
        }
        catch (NotFoundException | UncheckedIOException e) {
            throw new TrinoException(ICEBERG_INVALID_METADATA, "Error accessing manifest file for table %s".formatted(icebergTable.name()), e);
        }
    }

    private static List<ManifestFile> loadAllManifestsFromSnapshot(Table icebergTable, @Nullable Snapshot snapshot)
    {
        if (snapshot == null) {
            return ImmutableList.of();
        }
        try {
            return snapshot.allManifests(icebergTable.io());
        }
        catch (NotFoundException | UncheckedIOException e) {
            throw new TrinoException(ICEBERG_INVALID_METADATA, "Error accessing manifest file for table %s".formatted(icebergTable.name()), e);
        }
    }

    /**
     * Use instead of loadAllManifestsFromSnapshot when loading manifests from multiple distinct snapshots
     * Each BaseSnapshot object caches manifest files separately, so loading manifests from multiple distinct snapshots
     * results in O(num_snapshots^2) copies of the same manifest file metadata in memory
     */
    private static List<ManifestFile> loadAllManifestsFromManifestList(Table icebergTable, String manifestListLocation)
    {
        try {
            return IcebergManifestUtils.read(icebergTable.io(), manifestListLocation);
        }
        catch (NotFoundException | UncheckedIOException e) {
            throw new TrinoException(ICEBERG_INVALID_METADATA, "Error accessing manifest file for table %s".formatted(icebergTable.name()), e);
        }
    }

    private static Set<Integer> identityPartitionColumnsInAllSpecs(Table table)
    {
        // Extract identity partition column source ids common to ALL specs
        return table.spec().fields().stream()
                .filter(field -> field.transform().isIdentity())
                .filter(field -> table.specs().values().stream().allMatch(spec -> spec.fields().contains(field)))
                .map(PartitionField::sourceId)
                .collect(toImmutableSet());
    }

    @Override
    public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorExpression> projections,
            Map<String, ColumnHandle> assignments)
    {
        if (!isProjectionPushdownEnabled(session)) {
            return Optional.empty();
        }

        // Create projected column representations for supported sub expressions. Simple column references and chain of
        // dereferences on a variable are supported right now.
        Set<ConnectorExpression> projectedExpressions = projections.stream()
                .flatMap(expression -> extractSupportedProjectedColumns(expression).stream())
                .collect(toImmutableSet());

        Map<ConnectorExpression, ProjectedColumnRepresentation> columnProjections = projectedExpressions.stream()
                .collect(toImmutableMap(identity(), ApplyProjectionUtil::createProjectedColumnRepresentation));

        IcebergTableHandle icebergTableHandle = (IcebergTableHandle) handle;

        // all references are simple variables
        if (columnProjections.values().stream().allMatch(ProjectedColumnRepresentation::isVariable)) {
            Set<IcebergColumnHandle> projectedColumns = assignments.values().stream()
                    .map(IcebergColumnHandle.class::cast)
                    .collect(toImmutableSet());
            if (icebergTableHandle.getProjectedColumns().equals(projectedColumns)) {
                return Optional.empty();
            }
            List<Assignment> assignmentsList = assignments.entrySet().stream()
                    .map(assignment -> new Assignment(
                            assignment.getKey(),
                            assignment.getValue(),
                            ((IcebergColumnHandle) assignment.getValue()).getType()))
                    .collect(toImmutableList());

            return Optional.of(new ProjectionApplicationResult<>(
                    icebergTableHandle.withProjectedColumns(projectedColumns),
                    projections,
                    assignmentsList,
                    false));
        }

        Map<String, Assignment> newAssignments = new HashMap<>();
        ImmutableMap.Builder<ConnectorExpression, Variable> newVariablesBuilder = ImmutableMap.builder();
        ImmutableSet.Builder<IcebergColumnHandle> projectedColumnsBuilder = ImmutableSet.builder();

        for (Entry<ConnectorExpression, ProjectedColumnRepresentation> entry : columnProjections.entrySet()) {
            ConnectorExpression expression = entry.getKey();
            ProjectedColumnRepresentation projectedColumn = entry.getValue();

            IcebergColumnHandle baseColumnHandle = (IcebergColumnHandle) assignments.get(projectedColumn.getVariable().getName());
            IcebergColumnHandle projectedColumnHandle = createProjectedColumnHandle(baseColumnHandle, projectedColumn.getDereferenceIndices(), expression.getType());
            String projectedColumnName = projectedColumnHandle.getQualifiedName();

            Variable projectedColumnVariable = new Variable(projectedColumnName, expression.getType());
            Assignment newAssignment = new Assignment(projectedColumnName, projectedColumnHandle, expression.getType());
            newAssignments.putIfAbsent(projectedColumnName, newAssignment);

            newVariablesBuilder.put(expression, projectedColumnVariable);
            projectedColumnsBuilder.add(projectedColumnHandle);
        }

        // Modify projections to refer to new variables
        Map<ConnectorExpression, Variable> newVariables = newVariablesBuilder.buildOrThrow();
        List<ConnectorExpression> newProjections = projections.stream()
                .map(expression -> replaceWithNewVariables(expression, newVariables))
                .collect(toImmutableList());

        List<Assignment> outputAssignments = ImmutableList.copyOf(newAssignments.values());
        return Optional.of(new ProjectionApplicationResult<>(
                icebergTableHandle.withProjectedColumns(projectedColumnsBuilder.build()),
                newProjections,
                outputAssignments,
                false));
    }

    private static IcebergColumnHandle createProjectedColumnHandle(IcebergColumnHandle column, List<Integer> indices, io.trino.spi.type.Type projectedColumnType)
    {
        if (indices.isEmpty()) {
            return column;
        }
        ImmutableList.Builder<Integer> fullPath = ImmutableList.builder();
        fullPath.addAll(column.getPath());

        ColumnIdentity projectedColumnIdentity = column.getColumnIdentity();
        for (int index : indices) {
            // Position based lookup, not FieldId based
            projectedColumnIdentity = projectedColumnIdentity.getChildren().get(index);
            fullPath.add(projectedColumnIdentity.getId());
        }

        return IcebergColumnHandle.optional(column.getBaseColumnIdentity())
                .fieldType(column.getBaseType(), projectedColumnType)
                .path(fullPath.build())
                .build();
    }

    @Override
    public Optional<UnificationResult<ConnectorTableHandle>> unifyTables(ConnectorSession session, ConnectorTableHandle first, ConnectorTableHandle second)
    {
        IcebergTableHandle firstTable = (IcebergTableHandle) first;
        IcebergTableHandle secondTable = (IcebergTableHandle) second;

        if (!similar(firstTable, secondTable)) {
            return Optional.empty();
        }

        // create a unified table handle containing all columns from the first and second table handles
        IcebergTableHandle unified = new IcebergTableHandle(
                firstTable.getSchemaName(),
                firstTable.getTableName(),
                firstTable.getTableType(),
                firstTable.getSnapshotId(),
                firstTable.getTableSchemaJson(),
                firstTable.getSpecId(),
                firstTable.getPartitionSpecJsons(),
                firstTable.getFormatVersion(),
                TupleDomain.all(),
                TupleDomain.all(),
                OptionalLong.empty(),
                false,
                Sets.union(firstTable.getProjectedColumns(), secondTable.getProjectedColumns()),
                firstTable.getNameMappingJson(),
                firstTable.getTableLocation(),
                firstTable.getStorageProperties(),
                firstTable.getTablePartitioning(),
                firstTable.getBranch(),
                firstTable.isRecordScannedFiles(),
                firstTable.getMaxScannedFileSize(),
                firstTable.isForceReadingAllFiles(),
                ImmutableSet.of(),
                firstTable.getForAnalyze());

        // union and push enforced predicates from the first and second table handles
        // it doesn't matter if the unioned TupleDomain is abundant or if pushdown is incomplete
        // the original enforced predicates for both tables will be returned to the caller to re-apply
        // Note: the pushed predicate might use columns that are not present in projectedColumns
        TupleDomain<ColumnHandle> unionedEnforcedConstraint = columnWiseUnion(
                firstTable.getEnforcedPredicate()
                        .transformKeys(ColumnHandle.class::cast),
                secondTable.getEnforcedPredicate()
                        .transformKeys(ColumnHandle.class::cast));
        Optional<ConstraintApplicationResult<ConnectorTableHandle>> enforcedResult = applyFilter(
                session,
                unified,
                new Constraint(unionedEnforcedConstraint, unionedEnforcedConstraint.asPredicate(), unionedEnforcedConstraint.getDomains().map(Map::keySet).orElse(ImmutableSet.of())));
        if (enforcedResult.isPresent()) {
            unified = (IcebergTableHandle) enforcedResult.get().getHandle();
        }

        // union and push unenforced predicates from the first and second table handles
        // it doesn't matter if the unioned TupleDomain is abundant or if pushdown is incomplete
        // the unenforced predicate is "best effort", and there is no guarantee it is effective
        // Note: the pushed predicate might use columns that are not present in projectedColumns
        TupleDomain<ColumnHandle> unionedUnenforcedConstraint = columnWiseUnion(
                firstTable.getUnenforcedPredicate()
                        .transformKeys(ColumnHandle.class::cast),
                secondTable.getUnenforcedPredicate()
                        .transformKeys(ColumnHandle.class::cast));
        Optional<ConstraintApplicationResult<ConnectorTableHandle>> unenforcedResult = applyFilter(
                session,
                unified,
                new Constraint(unionedUnenforcedConstraint, unionedUnenforcedConstraint.asPredicate(), unionedUnenforcedConstraint.getDomains().map(Map::keySet).orElse(ImmutableSet.of())));
        if (unenforcedResult.isPresent()) {
            unified = (IcebergTableHandle) unenforcedResult.get().getHandle();
        }

        TupleDomain<IcebergColumnHandle> firstCompensationFilter = firstTable.getEnforcedPredicate().contains(unified.getEnforcedPredicate()) ? TupleDomain.all() : firstTable.getEnforcedPredicate();
        TupleDomain<IcebergColumnHandle> secondCompensationFilter = secondTable.getEnforcedPredicate().contains(unified.getEnforcedPredicate()) ? TupleDomain.all() : secondTable.getEnforcedPredicate();

        // union and push limits from the first and second table handles
        // we can do this only if we're not extracting and returning to the engine any compensating filters for the unified table handles
        // the compensating filters are applied later by the engine, which would mean that we pulled filter above limit
        if (firstCompensationFilter.isAll() && secondCompensationFilter.isAll() && firstTable.getLimit().isPresent() && secondTable.getLimit().isPresent()) {
            Optional<LimitApplicationResult<ConnectorTableHandle>> limitResult = applyLimit(session, unified, Long.max(firstTable.getLimit().getAsLong(), secondTable.getLimit().getAsLong()));
            if (limitResult.isPresent()) {
                unified = (IcebergTableHandle) limitResult.get().getHandle();
            }
        }

        // expose all columns necessary to support the compensation filters
        Set<IcebergColumnHandle> compensationColumns = Stream.of(firstCompensationFilter, secondCompensationFilter)
                .map(TupleDomain::getDomains)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(Map::keySet)
                .flatMap(Set::stream)
                .collect(toImmutableSet());

        return Optional.of(new UnificationResult<>(
                unified.withProjectedColumns(Sets.union(unified.getProjectedColumns(), compensationColumns)),
                firstCompensationFilter.transformKeys(ColumnHandle.class::cast),
                secondCompensationFilter.transformKeys(ColumnHandle.class::cast),
                new UnificationResult.Properties(
                        unified.getEnforcedPredicate().transformKeys(ColumnHandle.class::cast),
                        TRUE,
                        emptyMap(),
                        // limit is not guaranteed by iceberg connector
                        OptionalLong.empty())));
    }

    /**
     * Check if tables can be unified. For unification, all properties of the compared tables must be equal,
     * except for: unenforced predicate, enforced predicate, constraint columns, limit, and projected columns.
     */
    private boolean similar(IcebergTableHandle first, IcebergTableHandle second)
    {
        return Objects.equals(first.getSchemaTableName(), second.getSchemaTableName()) &&
                first.getTableType() == second.getTableType() &&
                Objects.equals(first.getSnapshotId(), second.getSnapshotId()) &&
                Objects.equals(first.getTableSchemaJson(), second.getTableSchemaJson()) &&
                first.getFormatVersion() == second.getFormatVersion() &&
                Objects.equals(first.getTableLocation(), second.getTableLocation()) &&
                Objects.equals(first.getStorageProperties(), second.getStorageProperties()) &&
                Objects.equals(first.getNameMappingJson(), second.getNameMappingJson()) &&
                Objects.equals(first.getTablePartitioning(), second.getTablePartitioning()) &&
                first.isRecordScannedFiles() == second.isRecordScannedFiles() &&
                Objects.equals(first.getMaxScannedFileSize(), second.getMaxScannedFileSize()) &&
                first.isForceReadingAllFiles() == second.isForceReadingAllFiles() &&
                Objects.equals(first.getForAnalyze(), second.getForAnalyze());
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        if (!isStatisticsEnabled(session)) {
            return TableStatistics.empty();
        }

        IcebergTableHandle originalHandle = (IcebergTableHandle) tableHandle;

        if (originalHandle.isRecordScannedFiles()) {
            return TableStatistics.empty();
        }
        // Certain table handle attributes are not applicable to select queries (which need stats).
        // If this changes, the caching logic may here may need to be revised.
        checkArgument(originalHandle.getMaxScannedFileSize().isEmpty(), "Unexpected max scanned file size set");

        if (originalHandle.getProjectedColumns().contains(rowIdColumnHandle()) || originalHandle.getProjectedColumns().contains(lastUpdatedSequenceNumberColumnHandle())) {
            log.warn("Statistics for $row_id and $last_updated_sequence_number columns are not supported");
            return TableStatistics.empty();
        }

        IcebergTableHandle cacheKey = new IcebergTableHandle(
                originalHandle.getSchemaName(),
                originalHandle.getTableName(),
                originalHandle.getTableType(),
                originalHandle.getSnapshotId(),
                originalHandle.getTableSchemaJson(),
                originalHandle.getSpecId(),
                originalHandle.getPartitionSpecJsons(),
                originalHandle.getFormatVersion(),
                originalHandle.getUnenforcedPredicate(),
                // Skip $file_modified_time in cache key as the statistics do not depend on it
                originalHandle.getEnforcedPredicate().filter((column, _) -> FILE_MODIFIED_TIME.getId() != column.getId()),
                OptionalLong.empty(), // limit is currently not included in stats and is not enforced by the connector
                false, // preferSmallInitialReads does not affect stats
                ImmutableSet.of(), // projectedColumns are used to request statistics only for the required columns, but are not part of cache key
                originalHandle.getNameMappingJson(),
                originalHandle.getTableLocation(),
                originalHandle.getStorageProperties(),
                Optional.empty(), // requiredTablePartitioning does not affect stats
                originalHandle.getBranch(),
                false, // recordScannedFiles does not affect stats
                originalHandle.getMaxScannedFileSize(),
                originalHandle.isForceReadingAllFiles(),
                ImmutableSet.of(), // constraintColumns do not affect stats
                Optional.empty()); // forAnalyze does not affect stats
        return getIncrementally(
                tableStatisticsCache,
                cacheKey,
                currentStatistics -> currentStatistics.getColumnStatistics().keySet().containsAll(originalHandle.getProjectedColumns()),
                projectedColumns -> {
                    Table icebergTable = catalog.loadTable(session, originalHandle.getSchemaTableName());
                    return tableStatisticsReader.getTableStatistics(
                            session,
                            originalHandle,
                            projectedColumns,
                            icebergTable);
                },
                originalHandle.getProjectedColumns());
    }

    @Override
    public void setTableAuthorization(ConnectorSession session, SchemaTableName tableName, TrinoPrincipal principal)
    {
        catalog.setTablePrincipal(session, tableName, principal);
    }

    private OptionalLong getCurrentSnapshotId(Table table)
    {
        if (table.currentSnapshot() == null) {
            return OptionalLong.empty();
        }
        return OptionalLong.of(table.currentSnapshot().snapshotId());
    }

    Table getIcebergTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        return catalog.loadTable(session, schemaTableName);
    }

    @Override
    public void createMaterializedView(
            ConnectorSession session,
            SchemaTableName viewName,
            ConnectorMaterializedViewDefinition definition,
            Map<String, Object> properties,
            boolean replace,
            boolean ignoreExisting)
    {
        validateRefreshInterval((String) properties.get(REFRESH_SCHEDULE));
        catalog.createMaterializedView(session, viewName, definition, properties, replace, ignoreExisting);
    }

    @Override
    public void dropMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        catalog.dropMaterializedView(session, viewName);
    }

    @Override
    public boolean delegateMaterializedViewRefreshToConnector(ConnectorSession session, SchemaTableName viewName)
    {
        return false;
    }

    @Override
    public ConnectorInsertTableHandle beginRefreshMaterializedView(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            List<ConnectorTableHandle> sourceTableHandles,
            boolean hasForeignSourceTables,
            RetryMode retryMode,
            RefreshType refreshType)
    {
        checkState(fromSnapshotForRefresh.isEmpty(), "From Snapshot must be empty at the start of MV refresh operation.");
        IcebergTableHandle table = (IcebergTableHandle) tableHandle;
        Table icebergTable = catalog.loadTable(session, table.getSchemaTableName());
        beginTransaction(icebergTable);

        Optional<String> dependencies = Optional.ofNullable(icebergTable.currentSnapshot())
                .map(Snapshot::summary)
                .map(summary -> summary.get(DEPENDS_ON_TABLES));

        boolean shouldUseIncremental = isIncrementalRefreshEnabled(session)
                && refreshType == RefreshType.INCREMENTAL
                // there is a single source table
                && sourceTableHandles.size() == 1
                // and there are no other foreign sources
                && !hasForeignSourceTables
                // and the source table's fromSnapshot is available in the MV snapshot summary
                && dependencies.isPresent() && !dependencies.get().equals(UNKNOWN_SNAPSHOT_TOKEN);

        if (shouldUseIncremental) {
            Map<String, String> sourceTableToSnapshot = MAP_SPLITTER.split(dependencies.get());
            checkState(sourceTableToSnapshot.size() == 1, "Expected %s to contain only single source table in snapshot summary", sourceTableToSnapshot);
            Entry<String, String> sourceTable = getOnlyElement(sourceTableToSnapshot.entrySet());
            String[] schemaTable = sourceTable.getKey().split("\\.");
            IcebergTableHandle handle = (IcebergTableHandle) getOnlyElement(sourceTableHandles);
            SchemaTableName sourceSchemaTable = new SchemaTableName(schemaTable[0], schemaTable[1]);
            checkState(sourceSchemaTable.equals(handle.getSchemaTableName()), "Source table name %s doesn't match handle table name %s", sourceSchemaTable, handle.getSchemaTableName());
            fromSnapshotForRefresh = OptionalLong.of(Long.parseLong(sourceTable.getValue()));
        }

        List<PositionDeleteFiles> rewritableDeletes = loadPreviousDeleteFiles(icebergTable, Optional.empty());
        return newWritableTableHandle(table.getSchemaTableName(), icebergTable, table.getTableSchemaJson(), Optional.empty(), rewritableDeletes);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishRefreshMaterializedView(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            ConnectorInsertTableHandle insertHandle,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics,
            List<ConnectorTableHandle> sourceTableHandles,
            boolean hasForeignSourceTables,
            boolean hasSourceTableFunctions,
            boolean hasNonDeterministicFunctions)
    {
        IcebergWritableTableHandle table = (IcebergWritableTableHandle) insertHandle;

        Table icebergTable = transaction.table();
        boolean isFullRefresh = fromSnapshotForRefresh.isEmpty();
        if (isFullRefresh) {
            // delete before insert .. simulating overwrite
            log.info("Performing full MV refresh for storage table: %s", table.name());
            transaction.newDelete()
                    .deleteFromRowFilter(Expressions.alwaysTrue())
                    .scanManifestsWith(icebergScanExecutor)
                    .commit();
        }
        else {
            log.info("Performing incremental MV refresh for storage table: %s", table.name());
        }

        List<CommitTaskData> commitTasks = fragments.stream()
                .map(Slice::getInput)
                .map(commitTaskCodec::fromJson)
                .collect(toImmutableList());

        Type[] partitionColumnTypes = icebergTable.spec().fields().stream()
                .map(field -> field.transform().getResultType(
                        icebergTable.schema().findType(field.sourceId())))
                .toArray(Type[]::new);

        AppendFiles appendFiles = isMergeManifestsOnWrite(session) ? transaction.newAppend() : transaction.newFastAppend();
        Map<Integer, SortOrder> sortOrders = icebergTable.sortOrders();
        for (CommitTaskData task : commitTasks) {
            DataFiles.Builder builder = DataFiles.builder(icebergTable.spec())
                    .withPath(task.path())
                    .withFileSizeInBytes(task.fileSizeInBytes())
                    .withFormat(table.fileFormat().toIceberg())
                    .withMetrics(task.metrics().metrics())
                    .withSortOrder(sortOrders.get(task.sortOrderId()));
            task.fileSplitOffsets().ifPresent(builder::withSplitOffsets);

            if (!icebergTable.spec().fields().isEmpty()) {
                String partitionDataJson = task.partitionDataJson()
                        .orElseThrow(() -> new VerifyException("No partition data for partitioned table"));
                builder.withPartition(PartitionData.fromJson(partitionDataJson, partitionColumnTypes));
            }

            appendFiles.appendFile(builder.build());
        }

        List<String> tableDependencies = new ArrayList<>();
        sourceTableHandles.stream()
                .map(handle -> {
                    if (!(handle instanceof IcebergTableHandle icebergHandle)) {
                        return UNKNOWN_SNAPSHOT_TOKEN;
                    }
                    return "%s=%s".formatted(
                            icebergHandle.getSchemaTableName(),
                            icebergHandle.getSnapshotId().isPresent() ? Long.toString(icebergHandle.getSnapshotId().getAsLong()) : "");
                })
                .forEach(tableDependencies::add);
        if (hasForeignSourceTables) {
            tableDependencies.add(UNKNOWN_SNAPSHOT_TOKEN);
        }

        // Update the 'dependsOnTables' property that tracks tables on which the materialized view depends and the corresponding snapshot ids of the tables
        appendFiles.set(DEPENDS_ON_TABLES, String.join(",", tableDependencies));
        appendFiles.set(DEPENDS_ON_TABLE_FUNCTIONS, String.valueOf(hasSourceTableFunctions));
        appendFiles.set(DEPENDS_ON_NON_DETERMINISTIC_FUNCTIONS, String.valueOf(hasNonDeterministicFunctions));
        appendFiles.set(TRINO_QUERY_START_TIME, session.getStart().toString());
        appendFiles.scanManifestsWith(icebergScanExecutor);
        commitUpdate(appendFiles, session, "refresh materialized view");

        if (isS3Tables(icebergTable.location())) {
            log.debug("S3 Tables does not support statistics: %s", table.name());
        }
        else if (!computedStatistics.isEmpty()) {
            long snapshotId = icebergTable.currentSnapshot().snapshotId();
            CollectedStatistics collectedStatistics = processComputedTableStatistics(icebergTable, computedStatistics);
            StatisticsFile statisticsFile = tableStatisticsWriter.writeStatisticsFile(
                    session,
                    icebergTable,
                    snapshotId,
                    isFullRefresh ? REPLACE : INCREMENTAL_UPDATE,
                    collectedStatistics);
            transaction.updateStatistics()
                    .setStatistics(statisticsFile)
                    .commit();
            partitionStatisticsWriter.writePartitionStats(session, icebergTable, snapshotId)
                    .ifPresent(partitionStatisticsFile -> transaction.updatePartitionStatistics()
                            .setPartitionStatistics(partitionStatisticsFile)
                            .commit());
        }
        commitTransaction(transaction, "refresh materialized view");

        transaction = null;
        fromSnapshotForRefresh = OptionalLong.empty();
        Map<String, String> summary = icebergTable.currentSnapshot().summary();
        Optional<ConnectorOutputMetadata> icebergCommitMetadata = summary == null ? Optional.empty() : Optional.of(new IcebergCommitMetadata(summary));

        if (materializedViewRefreshMaxSnapshotsToExpire == 0) {
            return icebergCommitMetadata;
        }

        int snapshots = size(icebergTable.snapshots());
        int snapshotsToRetain = max(1, snapshots - materializedViewRefreshMaxSnapshotsToExpire);
        try {
            executeExpireSnapshots(
                    session,
                    ((IcebergTableHandle) tableHandle).getSchemaTableName(),
                    materializedViewRefreshSnapshotRetentionPeriod,
                    ZERO,
                    snapshotsToRetain,
                    false);
        }
        catch (Exception e) {
            log.error(e, "Failed to delete old snapshot files during materialized view refresh");
        }

        return icebergCommitMetadata;
    }

    @Override
    public List<SchemaTableName> listMaterializedViews(ConnectorSession session, Optional<String> schemaName)
    {
        return catalog.listTables(session, schemaName).stream()
                .filter(info -> info.extendedRelationType() == TableInfo.ExtendedRelationType.TRINO_MATERIALIZED_VIEW)
                .map(TableInfo::tableName)
                .toList();
    }

    @Override
    public Map<SchemaTableName, ConnectorMaterializedViewDefinition> getMaterializedViews(ConnectorSession session, Optional<String> schemaName)
    {
        Map<SchemaTableName, ConnectorMaterializedViewDefinition> materializedViews = new HashMap<>();
        for (SchemaTableName name : listMaterializedViews(session, schemaName)) {
            try {
                getMaterializedView(session, name).ifPresent(view -> materializedViews.put(name, view));
            }
            catch (RuntimeException e) {
                // Materialized view can be being removed and this may cause all sorts of exceptions. Log, because we're catching broadly.
                log.warn(e, "Failed to access metadata of materialized view %s during listing", name);
            }
        }
        return materializedViews;
    }

    @Override
    public Optional<ConnectorMaterializedViewDefinition> getMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        return catalog.getMaterializedView(session, viewName);
    }

    @Override
    public Map<String, Object> getMaterializedViewProperties(ConnectorSession session, SchemaTableName viewName, ConnectorMaterializedViewDefinition definition)
    {
        return catalog.getMaterializedViewProperties(session, viewName, definition);
    }

    @Override
    public void renameMaterializedView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        // TODO (https://github.com/trinodb/trino/issues/9594) support rename across schemas
        if (!source.getSchemaName().equals(target.getSchemaName())) {
            throw new TrinoException(NOT_SUPPORTED, "Materialized View rename across schemas is not supported");
        }
        catalog.renameMaterializedView(session, source, target);
    }

    @Override
    public void setMaterializedViewProperties(ConnectorSession session, SchemaTableName viewName, Map<String, Optional<Object>> properties)
    {
        Set<String> unsupportedProperties = difference(properties.keySet(), UPDATABLE_MATERIALIZED_VIEW_PROPERTIES);
        if (!unsupportedProperties.isEmpty()) {
            throw new TrinoException(NOT_SUPPORTED, "The following properties cannot be updated: " + String.join(", ", unsupportedProperties));
        }

        Optional<String> refreshInterval = properties.get(REFRESH_SCHEDULE).map(String.class::cast);
        refreshInterval.ifPresent(IcebergMetadata::validateRefreshInterval);
        catalog.updateMaterializedViewRefreshSchedule(session, viewName, refreshInterval);
    }

    @Override
    public MaterializedViewFreshness getMaterializedViewFreshness(ConnectorSession session, SchemaTableName materializedViewName, boolean considerGracePeriod)
    {
        Optional<ConnectorMaterializedViewDefinition> materializedViewDefinition = getMaterializedView(session, materializedViewName);
        if (materializedViewDefinition.isEmpty()) {
            // View not found, might have been concurrently deleted
            return new MaterializedViewFreshness(STALE, Optional.empty());
        }

        SchemaTableName storageTableName = materializedViewDefinition.get().getStorageTable()
                .map(CatalogSchemaTableName::getSchemaTableName)
                .orElseThrow(() -> new IllegalStateException("Storage table missing in definition of materialized view " + materializedViewName));

        Table icebergTable = catalog.loadTable(session, storageTableName);
        Optional<Snapshot> currentSnapshot = Optional.ofNullable(icebergTable.currentSnapshot());
        String dependsOnTables = currentSnapshot
                .map(snapshot -> snapshot.summary().getOrDefault(DEPENDS_ON_TABLES, ""))
                .orElse("");
        boolean dependsOnTableFunctions = currentSnapshot
                .map(snapshot -> Boolean.valueOf(snapshot.summary().getOrDefault(DEPENDS_ON_TABLE_FUNCTIONS, "false")))
                .orElse(false);
        // For MVs refreshed before non-deterministic function tracking was added this flag
        // defaults to false. Such MVs will be correctly flagged after their next refresh.
        boolean dependsOnNonDeterministicFunctions = currentSnapshot
                .map(snapshot -> Boolean.valueOf(snapshot.summary().getOrDefault(DEPENDS_ON_NON_DETERMINISTIC_FUNCTIONS, "false")))
                .orElse(false);

        Optional<Instant> refreshStartTime = currentSnapshot.map(snapshot -> snapshot.summary().get(TRINO_QUERY_START_TIME))
                .map(Instant::parse);
        Optional<Instant> refreshTime = refreshStartTime
                // Fallback to snapshot commit time (end of refresh) for MVs defined before TRINO_QUERY_START_TIME was introduced
                .or(() -> currentSnapshot.map(snapshot -> Instant.ofEpochMilli(snapshot.timestampMillis())));

        if (dependsOnTableFunctions) {
            // It can't be determined whether a value returned by table function is STALE or not
            return new MaterializedViewFreshness(UNKNOWN, refreshTime);
        }

        if (dependsOnNonDeterministicFunctions) {
            // Non-deterministic functions like current_timestamp produce different values over time,
            // so the materialized view may be stale even if base tables haven't changed
            return new MaterializedViewFreshness(UNKNOWN, refreshTime);
        }

        if (dependsOnTables.isEmpty()) {
            // Information missing. While it's "unknown" whether storage is stale, we return "stale".
            // Normally dependsOnTables may be missing only when there was no refresh yet.
            return new MaterializedViewFreshness(STALE, Optional.empty());
        }

        Optional<java.time.Duration> gracePeriod = materializedViewDefinition.get().getGracePeriod();
        if (considerGracePeriod && withinGracePeriod(session.getStart(), refreshStartTime, gracePeriod)) {
            // To determine freshness, we normally load current metadata for each base table and check if there
            // is a newer snapshot than the recorded one (DEPENDS_ON_TABLES). This requires expensive metastore
            // operations for each base Iceberg table.
            //
            // The refresh query can read base table snapshots created before or during its execution. In the most
            // pessimistic scenario, a new base table snapshot is created immediately after the refresh started
            // (at refreshStartTime + epsilon), but the refresh reads an older snapshot. This new snapshot would
            // not be recorded in DEPENDS_ON_TABLES, making the MV technically stale. However, when the caller set
            // considerGracePeriod to true and refreshStartTime + gracePeriod > referenceTime, we can safely say that
            // the MV is at least within the grace period because refreshStartTime is before the new snapshot creation time.
            return new MaterializedViewFreshness(FRESH_WITHIN_GRACE_PERIOD, Optional.empty());
        }

        boolean hasUnknownTables = false;
        OptionalLong firstTableChange = OptionalLong.of(Long.MAX_VALUE);
        ImmutableList.Builder<Callable<TableChangeInfo>> tableChangeInfoTasks = ImmutableList.builder();
        for (String tableToSnapShot : Splitter.on(',').split(dependsOnTables)) {
            if (tableToSnapShot.equals(UNKNOWN_SNAPSHOT_TOKEN)) {
                hasUnknownTables = true;
                firstTableChange = OptionalLong.empty();
                continue;
            }

            tableChangeInfoTasks.add(() -> getTableChangeInfo(session, tableToSnapShot));
        }

        boolean hasStaleIcebergTables = false;
        List<TableChangeInfo> tableChangeInfos;

        try {
            tableChangeInfos = processWithAdditionalThreads(tableChangeInfoTasks.build(), metadataFetchingExecutor);
        }
        catch (ExecutionException e) {
            throw new RuntimeException(e.getCause());
        }

        verifyNotNull(tableChangeInfos);

        for (TableChangeInfo tableChangeInfo : tableChangeInfos) {
            switch (tableChangeInfo) {
                case NoTableChange() -> {
                    // Fresh
                }
                case FirstChangeSnapshot(Snapshot snapshot) -> {
                    hasStaleIcebergTables = true;
                    firstTableChange = firstTableChange.isPresent() ?
                            OptionalLong.of(Math.min(firstTableChange.getAsLong(), snapshot.timestampMillis())) :
                            OptionalLong.empty();
                }
                case UnknownTableChange(), GoneOrCorruptedTableChange() -> {
                    hasStaleIcebergTables = true;
                    firstTableChange = OptionalLong.empty();
                }
            }
        }

        Optional<Instant> lastKnownFreshTime = firstTableChange.isPresent() ? Optional.of(Instant.ofEpochMilli(firstTableChange.getAsLong())) : refreshTime;
        if (hasStaleIcebergTables) {
            return new MaterializedViewFreshness(STALE, lastKnownFreshTime);
        }
        if (hasUnknownTables) {
            return new MaterializedViewFreshness(UNKNOWN, lastKnownFreshTime);
        }
        return new MaterializedViewFreshness(FRESH, Optional.empty());
    }

    private boolean withinGracePeriod(Instant sessionStart, Optional<Instant> refreshStartTime, Optional<java.time.Duration> gracePeriod)
    {
        if (gracePeriod.isEmpty()) {
            // infinite grace period
            return true;
        }
        //noinspection OptionalIsPresent
        if (refreshStartTime.isEmpty()) {
            // refresh time unknown
            return false;
        }
        return refreshStartTime.get().plus(gracePeriod.get()).isAfter(sessionStart);
    }

    private TableChangeInfo getTableChangeInfo(ConnectorSession session, String entry)
    {
        List<String> keyValue = Splitter.on("=").splitToList(entry);
        if (keyValue.size() != 2) {
            throw new TrinoException(ICEBERG_INVALID_METADATA, format("Invalid entry in '%s' property: %s'", DEPENDS_ON_TABLES, entry));
        }
        String tableName = keyValue.get(0);
        String value = keyValue.get(1);
        List<String> strings = Splitter.on(".").splitToList(tableName);
        if (strings.size() == 3) {
            strings = strings.subList(1, 3);
        }
        else if (strings.size() != 2) {
            throw new TrinoException(ICEBERG_INVALID_METADATA, format("Invalid table name in '%s' property: %s'", DEPENDS_ON_TABLES, strings));
        }
        String schema = strings.get(0);
        String name = strings.get(1);
        SchemaTableName schemaTableName = new SchemaTableName(schema, name);
        ConnectorTableHandle tableHandle = getTableHandle(session, schemaTableName, Optional.empty(), Optional.empty());

        if (tableHandle == null || tableHandle instanceof CorruptedIcebergTableHandle) {
            // Base table is gone or table is corrupted
            return new GoneOrCorruptedTableChange();
        }
        OptionalLong snapshotAtRefresh;
        if (value.isEmpty()) {
            snapshotAtRefresh = OptionalLong.empty();
        }
        else {
            snapshotAtRefresh = OptionalLong.of(Long.parseLong(value));
        }
        return getTableChangeInfo(session, (IcebergTableHandle) tableHandle, snapshotAtRefresh);
    }

    private TableChangeInfo getTableChangeInfo(ConnectorSession session, IcebergTableHandle table, OptionalLong snapshotAtRefresh)
    {
        Table icebergTable = catalog.loadTable(session, table.getSchemaTableName());
        Snapshot currentSnapshot = icebergTable.currentSnapshot();

        if (snapshotAtRefresh.isEmpty()) {
            // Table had no snapshot at refresh time.
            if (currentSnapshot == null) {
                return new NoTableChange();
            }
            return firstSnapshot(icebergTable)
                    .<TableChangeInfo>map(FirstChangeSnapshot::new)
                    .orElse(new UnknownTableChange());
        }

        if (snapshotAtRefresh.getAsLong() == currentSnapshot.snapshotId()) {
            return new NoTableChange();
        }
        return firstSnapshotAfter(icebergTable, snapshotAtRefresh.getAsLong())
                .<TableChangeInfo>map(FirstChangeSnapshot::new)
                .orElse(new UnknownTableChange());
    }

    @Override
    public void setColumnComment(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column, Optional<String> comment)
    {
        catalog.updateColumnComment(session, ((IcebergTableHandle) tableHandle).getSchemaTableName(), ((IcebergColumnHandle) column).getColumnIdentity(), comment);
    }

    @Override
    public Optional<CatalogSchemaTableName> redirectTable(ConnectorSession session, SchemaTableName tableName)
    {
        Optional<String> targetCatalogName = getHiveCatalogName(session);
        if (targetCatalogName.isEmpty()) {
            return Optional.empty();
        }
        return catalog.redirectTable(session, tableName, targetCatalogName.get());
    }

    @Override
    public boolean allowSplittingReadIntoMultipleSubQueries(ConnectorSession session, ConnectorTableHandle connectorTableHandle)
    {
        IcebergTableHandle tableHandle = (IcebergTableHandle) connectorTableHandle;
        IcebergFileFormat storageFormat = getFileFormat(tableHandle.getStorageProperties());

        return storageFormat == ORC || storageFormat == PARQUET;
    }

    @Override
    public WriterScalingOptions getNewTableWriterScalingOptions(ConnectorSession session, SchemaTableName tableName, Map<String, Object> tableProperties)
    {
        return WriterScalingOptions.ENABLED;
    }

    @Override
    public WriterScalingOptions getInsertWriterScalingOptions(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return WriterScalingOptions.ENABLED;
    }

    @Override
    public Optional<ConnectorTableHandle> applyPartialLimit(ConnectorSession session, ConnectorTableHandle handle, long limitHint)
    {
        IcebergTableHandle tableHandle = (IcebergTableHandle) handle;
        // Apply small reads per split optimization when limit hint is less than 100,000
        if (limitHint < 100_000 && !tableHandle.preferSmallInitialReads()) {
            return Optional.of(tableHandle.withPreferSmallInitialReads(true));
        }

        return Optional.empty();
    }

    public OptionalLong getIncrementalRefreshFromSnapshot()
    {
        return fromSnapshotForRefresh;
    }

    public void disableIncrementalRefresh()
    {
        fromSnapshotForRefresh = OptionalLong.empty();
    }

    private static CollectedStatistics processComputedTableStatistics(Table table, Collection<ComputedStatistics> computedStatistics)
    {
        Map<String, Integer> columnNameToId = table.schema().columns().stream()
                .collect(toImmutableMap(nestedField -> nestedField.name().toLowerCase(ENGLISH), Types.NestedField::fieldId));

        ImmutableMap.Builder<Integer, CompactThetaSketch> ndvSketches = ImmutableMap.builder();
        for (ComputedStatistics computedStatistic : computedStatistics) {
            verify(computedStatistic.getGroupingColumns().isEmpty() && computedStatistic.getGroupingValues().isEmpty(), "Unexpected grouping");
            verify(computedStatistic.getTableStatistics().isEmpty(), "Unexpected table statistics");
            for (Entry<ColumnStatisticMetadata, Block> entry : computedStatistic.getColumnStatistics().entrySet()) {
                ColumnStatisticMetadata statisticMetadata = entry.getKey();
                if (statisticMetadata.getConnectorAggregationId().equals(NUMBER_OF_DISTINCT_VALUES_NAME)) {
                    Integer columnId = verifyNotNull(
                            columnNameToId.get(statisticMetadata.getColumnName()),
                            "Column not found in table: [%s]",
                            statisticMetadata.getColumnName());
                    CompactThetaSketch sketch = DataSketchStateSerializer.deserialize(entry.getValue(), 0);
                    ndvSketches.put(columnId, sketch);
                }
                else {
                    throw new UnsupportedOperationException("Unsupported statistic: " + statisticMetadata);
                }
            }
        }

        return new CollectedStatistics(ndvSketches.buildOrThrow());
    }

    private void beginTransaction(Table icebergTable)
    {
        verify(transaction == null, "transaction already set");
        transaction = catalog.newTransaction(icebergTable);
    }

    private static IcebergTableHandle checkValidTableHandle(ConnectorTableHandle tableHandle)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        if (tableHandle instanceof CorruptedIcebergTableHandle corruptedTableHandle) {
            throw corruptedTableHandle.createException();
        }
        return ((IcebergTableHandle) tableHandle);
    }

    private sealed interface TableChangeInfo
            permits NoTableChange, FirstChangeSnapshot, UnknownTableChange, GoneOrCorruptedTableChange {}

    private record NoTableChange()
            implements TableChangeInfo {}

    private record FirstChangeSnapshot(Snapshot snapshot)
            implements TableChangeInfo
    {
        FirstChangeSnapshot
        {
            requireNonNull(snapshot, "snapshot is null");
        }
    }

    private record UnknownTableChange()
            implements TableChangeInfo {}

    private record GoneOrCorruptedTableChange()
            implements TableChangeInfo {}

    private static TableStatistics getIncrementally(
            Map<IcebergTableHandle, AtomicReference<TableStatistics>> cache,
            IcebergTableHandle key,
            Predicate<TableStatistics> isSufficient,
            Function<Set<IcebergColumnHandle>, TableStatistics> columnStatisticsLoader,
            Set<IcebergColumnHandle> projectedColumns)
    {
        AtomicReference<TableStatistics> valueHolder = cache.computeIfAbsent(key, _ -> new AtomicReference<>());
        TableStatistics oldValue = valueHolder.get();
        if (oldValue != null && isSufficient.test(oldValue)) {
            return oldValue;
        }

        TableStatistics newValue;
        if (oldValue == null) {
            newValue = columnStatisticsLoader.apply(projectedColumns);
        }
        else {
            Sets.SetView<IcebergColumnHandle> missingColumns = difference(projectedColumns, oldValue.getColumnStatistics().keySet());
            newValue = columnStatisticsLoader.apply(missingColumns);
        }

        verifyNotNull(newValue, "loader returned null for %s", key);

        TableStatistics merged = mergeColumnStatistics(oldValue, newValue);
        if (!valueHolder.compareAndSet(oldValue, merged)) {
            // if the value changed in the valueHolder, we only add newly loaded value to be sure we have up-to-date value
            valueHolder.accumulateAndGet(newValue, IcebergMetadata::mergeColumnStatistics);
        }
        return merged;
    }

    private static TableStatistics mergeColumnStatistics(TableStatistics currentStats, TableStatistics newStats)
    {
        requireNonNull(newStats, "newStats is null");
        TableStatistics.Builder statisticsBuilder = TableStatistics.builder();
        if (currentStats != null) {
            currentStats.getColumnStatistics().forEach(statisticsBuilder::setColumnStatistics);
        }
        statisticsBuilder.setRowCount(newStats.getRowCount());
        newStats.getColumnStatistics().forEach(statisticsBuilder::setColumnStatistics);
        return statisticsBuilder.build();
    }

    private static void validateRefreshInterval(String refreshInterval)
    {
        if (refreshInterval != null) {
            try {
                CRON_PARSER.parse(refreshInterval);
            }
            catch (RuntimeException e) {
                throw new TrinoException(CONFIGURATION_INVALID, "Refresh interval is not cron string", e);
            }
        }
    }

    private static RowLevelOperationMode rowLevelOperationMode(Table table)
    {
        // Trino uses MoR by default unlike Spark
        return RowLevelOperationMode.fromName(table.properties().getOrDefault(MERGE_MODE, MERGE_ON_READ.modeName()));
    }

    private static SnapshotRef checkBranch(Table table, String branch)
    {
        SnapshotRef ref = table.refs().get(branch);
        if (ref == null) {
            throw new TrinoException(BRANCH_NOT_FOUND, "Branch '%s' does not exist".formatted(branch));
        }
        if (ref.isTag()) {
            throw new TrinoException(NOT_SUPPORTED, "Branch '%s' does not exist, but a tag with that name exists".formatted(branch));
        }
        return ref;
    }

    private static void checkDefaultValueCompatibility(int formatVersion, ColumnMetadata column)
    {
        checkDefaultValueCompatibility(formatVersion, column.getDefaultValue().isPresent());
    }

    private static void checkDefaultValueCompatibility(int formatVersion, boolean defaultValuePresent)
    {
        if (formatVersion < 3 && defaultValuePresent) {
            throw new TrinoException(NOT_SUPPORTED, "Default column values are not supported for Iceberg table format version < 3");
        }
    }
}
