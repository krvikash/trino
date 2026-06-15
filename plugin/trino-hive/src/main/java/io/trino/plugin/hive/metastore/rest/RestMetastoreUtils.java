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
package io.trino.plugin.hive.metastore.rest;

import io.starburst.stargate.metastore.client.BasicStatistics;
import io.starburst.stargate.metastore.client.BucketProperty;
import io.starburst.stargate.metastore.client.ColumnStatistics;
import io.starburst.stargate.metastore.client.PartitionName;
import io.starburst.stargate.metastore.client.StatisticsUpdate;
import io.trino.metastore.BooleanStatistics;
import io.trino.metastore.Column;
import io.trino.metastore.Database;
import io.trino.metastore.DateStatistics;
import io.trino.metastore.DecimalStatistics;
import io.trino.metastore.DoubleStatistics;
import io.trino.metastore.HiveBasicStatistics;
import io.trino.metastore.HiveBucketProperty;
import io.trino.metastore.HiveColumnStatistics;
import io.trino.metastore.HiveType;
import io.trino.metastore.IntegerStatistics;
import io.trino.metastore.Partition;
import io.trino.metastore.PartitionStatistics;
import io.trino.metastore.PartitionWithStatistics;
import io.trino.metastore.SortingColumn;
import io.trino.metastore.SortingColumn.Order;
import io.trino.metastore.StatisticsUpdateMode;
import io.trino.metastore.Storage;
import io.trino.metastore.StorageFormat;
import io.trino.metastore.Table;
import io.trino.metastore.TableInfo;
import io.trino.spi.connector.SchemaTableName;

import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.hive.metastore.MetastoreUtil.updateStatisticsParameters;

final class RestMetastoreUtils
{
    static final String PUBLIC_ROLE_NAME = "public";

    private RestMetastoreUtils() {}

    public static Database fromRestDatabase(io.starburst.stargate.metastore.client.Database database)
    {
        return new Database(
                database.databaseName(),
                database.location(),
                Optional.empty(),
                Optional.empty(),
                database.comment(),
                database.parameters());
    }

    public static io.starburst.stargate.metastore.client.Database toRestDatabase(Database database)
    {
        return new io.starburst.stargate.metastore.client.Database(
                database.getDatabaseName(),
                database.getLocation(),
                database.getComment(),
                database.getParameters());
    }

    public static List<TableInfo> fromRestTableInfos(String databaseName, Collection<io.starburst.stargate.metastore.client.TableInfo> tableInfos)
    {
        return tableInfos.stream()
                .map(tableInfo -> fromRestTableInfo(databaseName, tableInfo))
                .collect(toImmutableList());
    }

    private static TableInfo fromRestTableInfo(String databaseName, io.starburst.stargate.metastore.client.TableInfo tableInfo)
    {
        return new TableInfo(
                new SchemaTableName(databaseName, tableInfo.tableName()),
                TableInfo.ExtendedRelationType.fromTableTypeAndComment(tableInfo.tableType(), tableInfo.comment().orElse(null)));
    }

    public static Table fromRestTable(io.starburst.stargate.metastore.client.Table table)
    {
        return new Table(
                table.databaseName(),
                table.tableName(),
                Optional.empty(),
                table.tableType(),
                fromRestStorage(table.storage()),
                fromRestColumns(table.dataColumns()),
                fromRestColumns(table.partitionColumns()),
                table.parameters(),
                table.viewOriginalText(),
                table.viewExpandedText(),
                OptionalLong.empty());
    }

    public static io.starburst.stargate.metastore.client.Table toRestTable(Table table)
    {
        return new io.starburst.stargate.metastore.client.Table(
                table.getDatabaseName(),
                table.getTableName(),
                table.getTableType(),
                toRestStorage(table.getStorage()),
                toRestColumns(table.getDataColumns()),
                toRestColumns(table.getPartitionColumns()),
                table.getParameters(),
                table.getViewOriginalText(),
                table.getViewExpandedText());
    }

    public static Partition fromRestPartition(io.starburst.stargate.metastore.client.Partition partition)
    {
        return new Partition(
                partition.databaseName(),
                partition.tableName(),
                partition.partitionName().partitionValues(),
                fromRestStorage(partition.storage()),
                fromRestColumns(partition.dataColumns()),
                partition.parameters());
    }

    public static io.starburst.stargate.metastore.client.Partition toRestPartition(Partition partition, HiveBasicStatistics basicStatistics)
    {
        return new io.starburst.stargate.metastore.client.Partition(
                partition.getDatabaseName(),
                partition.getTableName(),
                new PartitionName(partition.getValues()),
                toRestStorage(partition.getStorage()),
                toRestColumns(partition.getColumns()),
                updateStatisticsParameters(partition.getParameters(), basicStatistics));
    }

    public static io.starburst.stargate.metastore.client.PartitionWithStatistics toRestPartitionWithStatistics(PartitionWithStatistics partition)
    {
        return new io.starburst.stargate.metastore.client.PartitionWithStatistics(
                toRestPartition(partition.getPartition(), partition.getStatistics().basicStatistics()),
                partition.getStatistics().columnStatistics().entrySet().stream()
                        .collect(toImmutableMap(
                                Entry::getKey,
                                entry -> toRestColumnStatistics(entry.getValue()))));
    }

    private static Column fromRestColumn(io.starburst.stargate.metastore.client.Column column)
    {
        return new Column(column.name(), HiveType.valueOf(column.type()), column.comment(), column.properties());
    }

    private static List<Column> fromRestColumns(List<io.starburst.stargate.metastore.client.Column> columns)
    {
        return columns.stream()
                .map(RestMetastoreUtils::fromRestColumn)
                .collect(toImmutableList());
    }

    private static io.starburst.stargate.metastore.client.Column toRestColumn(Column column)
    {
        return new io.starburst.stargate.metastore.client.Column(column.getName(), column.getType().toString(), column.getComment(), column.getProperties());
    }

    private static List<io.starburst.stargate.metastore.client.Column> toRestColumns(List<Column> columns)
    {
        return columns.stream()
                .map(RestMetastoreUtils::toRestColumn)
                .collect(toImmutableList());
    }

    private static SortingColumn fromRestSortingColumn(io.starburst.stargate.metastore.client.SortingColumn column)
    {
        return new SortingColumn(column.columnName(), fromRestOrder(column.order()));
    }

    private static io.starburst.stargate.metastore.client.SortingColumn toRestSortingColumn(SortingColumn column)
    {
        return new io.starburst.stargate.metastore.client.SortingColumn(column.columnName(), toRestOrder(column.order()));
    }

    private static Order fromRestOrder(io.starburst.stargate.metastore.client.SortingColumn.Order order)
    {
        return switch (order) {
            case ASCENDING -> Order.ASCENDING;
            case DESCENDING -> Order.DESCENDING;
        };
    }

    private static io.starburst.stargate.metastore.client.SortingColumn.Order toRestOrder(Order order)
    {
        return switch (order) {
            case ASCENDING -> io.starburst.stargate.metastore.client.SortingColumn.Order.ASCENDING;
            case DESCENDING -> io.starburst.stargate.metastore.client.SortingColumn.Order.DESCENDING;
        };
    }

    private static Storage fromRestStorage(io.starburst.stargate.metastore.client.Storage storage)
    {
        Optional<HiveBucketProperty> bucketProperty = storage.bucketProperty()
                .map(RestMetastoreUtils::fromRestBucketProperty);
        return new Storage(
                fromRestStorageFormat(storage.storageFormat()),
                Optional.of(storage.location()),
                bucketProperty,
                storage.skewed(),
                storage.serdeParameters());
    }

    private static io.starburst.stargate.metastore.client.Storage toRestStorage(Storage storage)
    {
        Optional<BucketProperty> bucketProperty = storage.getBucketProperty()
                .map(RestMetastoreUtils::toRestBucketProperty);
        return new io.starburst.stargate.metastore.client.Storage(
                toRestStorageFormat(storage.getStorageFormat()),
                storage.getLocation(),
                bucketProperty,
                storage.isSkewed(),
                storage.getSerdeParameters());
    }

    private static HiveBucketProperty fromRestBucketProperty(BucketProperty bucketProperty)
    {
        return new HiveBucketProperty(
                bucketProperty.bucketedBy(),
                bucketProperty.bucketCount(),
                bucketProperty.sortedBy().stream()
                        .map(RestMetastoreUtils::fromRestSortingColumn)
                        .collect(toImmutableList()));
    }

    private static BucketProperty toRestBucketProperty(HiveBucketProperty bucketProperty)
    {
        return new BucketProperty(
                bucketProperty.bucketedBy(),
                bucketProperty.bucketCount(),
                bucketProperty.sortedBy().stream()
                        .map(RestMetastoreUtils::toRestSortingColumn)
                        .collect(toImmutableList()));
    }

    private static StorageFormat fromRestStorageFormat(io.starburst.stargate.metastore.client.StorageFormat storageFormat)
    {
        return StorageFormat.createNullable(storageFormat.serDe(), storageFormat.inputFormat(), storageFormat.outputFormat());
    }

    private static io.starburst.stargate.metastore.client.StorageFormat toRestStorageFormat(StorageFormat storageFormat)
    {
        return new io.starburst.stargate.metastore.client.StorageFormat(
                storageFormat.getSerDeNullable(),
                storageFormat.getInputFormatNullable(),
                storageFormat.getOutputFormatNullable());
    }

    public static io.starburst.stargate.metastore.client.StatisticsUpdateMode toRestStatisticsUpdateMode(StatisticsUpdateMode mode)
    {
        return switch (mode) {
            case OVERWRITE_ALL -> io.starburst.stargate.metastore.client.StatisticsUpdateMode.OVERWRITE_ALL;
            case OVERWRITE_SOME_COLUMNS -> io.starburst.stargate.metastore.client.StatisticsUpdateMode.OVERWRITE_SOME_COLUMNS;
            case MERGE_INCREMENTAL -> io.starburst.stargate.metastore.client.StatisticsUpdateMode.MERGE_INCREMENTAL;
            case UNDO_MERGE_INCREMENTAL -> io.starburst.stargate.metastore.client.StatisticsUpdateMode.UNDO_MERGE_INCREMENTAL;
            case CLEAR_ALL -> io.starburst.stargate.metastore.client.StatisticsUpdateMode.CLEAR_ALL;
        };
    }

    public static StatisticsUpdate toRestStatisticsUpdate(PartitionStatistics statistics)
    {
        return new StatisticsUpdate(
                toRestBasicStatistics(statistics.basicStatistics()),
                statistics.columnStatistics().entrySet().stream()
                        .collect(toImmutableMap(
                                Entry::getKey,
                                entry -> toRestColumnStatistics(entry.getValue()))));
    }

    private static BasicStatistics toRestBasicStatistics(HiveBasicStatistics statistics)
    {
        return new BasicStatistics(
                statistics.getFileCount(),
                statistics.getRowCount(),
                statistics.getInMemoryDataSizeInBytes(),
                statistics.getOnDiskDataSizeInBytes());
    }

    public static HiveColumnStatistics fromRestColumnStatistics(ColumnStatistics columnStatistics)
    {
        return new HiveColumnStatistics(
                columnStatistics.integerStatistics().map(RestMetastoreUtils::fromRestIntegerStatistics),
                columnStatistics.doubleStatistics().map(RestMetastoreUtils::fromRestDoubleStatistics),
                columnStatistics.decimalStatistics().map(RestMetastoreUtils::fromRestDecimalStatistics),
                columnStatistics.dateStatistics().map(RestMetastoreUtils::fromRestDateStatistics),
                columnStatistics.booleanStatistics().map(RestMetastoreUtils::fromRestBooleanStatistics),
                columnStatistics.maxValueSizeInBytes(),
                columnStatistics.averageColumnLength(),
                columnStatistics.nullsCount(),
                columnStatistics.distinctValuesWithNullCount());
    }

    private static ColumnStatistics toRestColumnStatistics(HiveColumnStatistics columnStatistics)
    {
        return new ColumnStatistics(
                columnStatistics.getIntegerStatistics().map(RestMetastoreUtils::toRestIntegerStatistics),
                columnStatistics.getDoubleStatistics().map(RestMetastoreUtils::toRestDoubleStatistics),
                columnStatistics.getDecimalStatistics().map(RestMetastoreUtils::toRestDecimalStatistics),
                columnStatistics.getDateStatistics().map(RestMetastoreUtils::toRestDateStatistics),
                columnStatistics.getBooleanStatistics().map(RestMetastoreUtils::toRestBooleanStatistics),
                columnStatistics.getMaxValueSizeInBytes(),
                columnStatistics.getAverageColumnLength(),
                columnStatistics.getNullsCount(),
                columnStatistics.getDistinctValuesWithNullCount());
    }

    private static IntegerStatistics fromRestIntegerStatistics(ColumnStatistics.IntegerStatistics integerStatistics)
    {
        return new IntegerStatistics(integerStatistics.min(), integerStatistics.max());
    }

    private static ColumnStatistics.IntegerStatistics toRestIntegerStatistics(IntegerStatistics integerStatistics)
    {
        return new ColumnStatistics.IntegerStatistics(integerStatistics.getMin(), integerStatistics.getMax());
    }

    private static DoubleStatistics fromRestDoubleStatistics(ColumnStatistics.DoubleStatistics doubleStatistics)
    {
        return new DoubleStatistics(doubleStatistics.min(), doubleStatistics.max());
    }

    private static ColumnStatistics.DoubleStatistics toRestDoubleStatistics(DoubleStatistics doubleStatistics)
    {
        return new ColumnStatistics.DoubleStatistics(doubleStatistics.getMin(), doubleStatistics.getMax());
    }

    private static DecimalStatistics fromRestDecimalStatistics(ColumnStatistics.DecimalStatistics decimalStatistics)
    {
        return new DecimalStatistics(decimalStatistics.min(), decimalStatistics.max());
    }

    private static ColumnStatistics.DecimalStatistics toRestDecimalStatistics(DecimalStatistics decimalStatistics)
    {
        return new ColumnStatistics.DecimalStatistics(decimalStatistics.getMin(), decimalStatistics.getMax());
    }

    private static DateStatistics fromRestDateStatistics(ColumnStatistics.DateStatistics dateStatistics)
    {
        return new DateStatistics(dateStatistics.min(), dateStatistics.max());
    }

    private static ColumnStatistics.DateStatistics toRestDateStatistics(DateStatistics dateStatistics)
    {
        return new ColumnStatistics.DateStatistics(dateStatistics.getMin(), dateStatistics.getMax());
    }

    private static BooleanStatistics fromRestBooleanStatistics(ColumnStatistics.BooleanStatistics booleanStatistics)
    {
        return new BooleanStatistics(booleanStatistics.trueCount(), booleanStatistics.falseCount());
    }

    private static ColumnStatistics.BooleanStatistics toRestBooleanStatistics(BooleanStatistics booleanStatistics)
    {
        return new ColumnStatistics.BooleanStatistics(booleanStatistics.getTrueCount(), booleanStatistics.getFalseCount());
    }
}
