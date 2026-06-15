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
package io.starburst.stargate.tablemaintenance.partitioned;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.starburst.stargate.tablemaintenance.OptimizeQueryIterator;
import io.starburst.stargate.tablemaintenance.PartitionColumnBasedOptimizeStatus;
import io.starburst.stargate.tablemaintenance.partitioned.UnoptimizedPartitionValue.ColumnValue;
import io.trino.spi.connector.CatalogSchemaTableName;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.starburst.stargate.tablemaintenance.MaintenanceQueryGenerators.doubleQuoteEscape;
import static io.starburst.stargate.tablemaintenance.MaintenanceQueryGenerators.singleQuoteEscape;
import static io.starburst.stargate.tablemaintenance.partitioned.ShowCreateTableQueryUtils.parseFormatVersion;
import static java.util.Objects.requireNonNull;
import static java.util.function.UnaryOperator.identity;

public class PartitionBasedMaintenanceIteratorFactory
{
    private static final Logger log = Logger.get(PartitionBasedMaintenanceIteratorFactory.class);
    private static final String GET_COLUMN_NAMES_QUERY =
            """
            SELECT
                column_name, data_type
            FROM
                "%s".information_schema.columns
            WHERE
                table_catalog = '%s'
                AND table_schema = '%s'
                AND table_name = '%s'
            ORDER BY
                ordinal_position ASC
            """;
    private static final String SHOW_CREATE_TABLE_QUERY =
            """
            SHOW CREATE TABLE "%s"."%s"."%s"
            """;
    private static final Pattern PARTITIONING_PROPERTY_EXTRACT_PATTERN = Pattern.compile("partitioning = ARRAY\\[(.*?)]", Pattern.DOTALL);
    private static final Pattern PARTITIONING_ENTRIES_EXTRACT_PATTERN = Pattern.compile("'([^']*)'");

    private final DeterminePartitionOptimizeQueryRunner determinePartitionOptimizeQueryRunner;
    private final DataSize optimizeFileSizeThreshold;

    public PartitionBasedMaintenanceIteratorFactory(
            DeterminePartitionOptimizeQueryRunner determinePartitionOptimizeQueryRunner,
            DataSize optimizeFileSizeThreshold)
    {
        this.determinePartitionOptimizeQueryRunner = requireNonNull(determinePartitionOptimizeQueryRunner, "determinePartitionOptimizeQueryRunner is null");
        this.optimizeFileSizeThreshold = requireNonNull(optimizeFileSizeThreshold, "optimizeFileSizeThreshold is null");
    }

    public OptionalInt queryFormatVersion(CatalogSchemaTableName tableName)
    {
        try {
            String showCreateTableQuery = SHOW_CREATE_TABLE_QUERY.formatted(
                    doubleQuoteEscape(tableName.getCatalogName()),
                    doubleQuoteEscape(tableName.getSchemaTableName().getSchemaName()),
                    doubleQuoteEscape(tableName.getSchemaTableName().getTableName()));
            String showCreateTableOutput = determinePartitionOptimizeQueryRunner.runShowCreateTableQuery(showCreateTableQuery);
            return parseFormatVersion(showCreateTableOutput);
        }
        catch (Exception e) {
            log.warn(e, "Failed to query format version for table: %s.%s.%s.".formatted(
                    tableName.getCatalogName(),
                    tableName.getSchemaTableName().getSchemaName(),
                    tableName.getSchemaTableName().getTableName()));
            return OptionalInt.empty();
        }
    }

    public Optional<PartitionedBasedMaintenanceDetails> createPartitionedTableOptimizeQueriesIterator(CatalogSchemaTableName tableName)
    {
        return createPartitionedTableOptimizeQueriesIterator(tableName, 1);
    }

    public Optional<PartitionedBasedMaintenanceDetails> createPartitionedTableOptimizeQueriesIterator(CatalogSchemaTableName tableName, int requestedNumberOfPartitionColumns)
    {
        checkArgument(requestedNumberOfPartitionColumns > 0, "requestedNumberOfPartitionColumns must be positive, got: %s", requestedNumberOfPartitionColumns);
        try {
            List<String> tablePartitionColumns = computeTablePartitionColumns(tableName);
            if (tablePartitionColumns.isEmpty()) {
                return Optional.empty();
            }
            List<TableColumn> columns = computeTableColumnNames(tableName);
            List<OptimizablePartitionColumn> optimizablePartitionColumns = findOptimizablePartitionColumns(columns, tablePartitionColumns);
            if (optimizablePartitionColumns.isEmpty()) {
                return Optional.empty();
            }
            int effectiveColumnCount = Math.min(requestedNumberOfPartitionColumns, optimizablePartitionColumns.size());
            if (effectiveColumnCount < requestedNumberOfPartitionColumns) {
                log.warn("Requested %d partition columns for table %s.%s.%s but only %d optimizable partition columns found, using %d.".formatted(
                        requestedNumberOfPartitionColumns,
                        tableName.getCatalogName(),
                        tableName.getSchemaTableName().getSchemaName(),
                        tableName.getSchemaTableName().getTableName(),
                        optimizablePartitionColumns.size(),
                        effectiveColumnCount));
            }
            List<OptimizablePartitionColumn> selectedFields = optimizablePartitionColumns.subList(0, effectiveColumnCount);
            return Optional.of(createOptimizeQueriesIteratorForPartitionColumns(tableName, selectedFields));
        }
        catch (Exception e) {
            log.warn(e, "Failed to create partitioned optimize queries for table: %s.%s.%s.".formatted(
                    tableName.getCatalogName(),
                    tableName.getSchemaTableName().getSchemaName(),
                    tableName.getSchemaTableName().getTableName()));
            return Optional.empty();
        }
    }

    private PartitionedBasedMaintenanceDetails createOptimizeQueriesIteratorForPartitionColumns(
            CatalogSchemaTableName tableName,
            List<OptimizablePartitionColumn> partitionColumns)
    {
        String unoptimizedPartitionColumnsQuery = buildUnoptimizedPartitionsQuery(tableName, partitionColumns);

        List<UnoptimizedPartitionValue> unoptimizedPartitionValues =
                determinePartitionOptimizeQueryRunner.runGetUnoptimizedPartitionColumnValuesQuery(
                        unoptimizedPartitionColumnsQuery, partitionColumns.size());

        List<String> partitionOptimizeQueries = unoptimizedPartitionValues.stream()
                .map(partitionValue -> buildWherePredicate(partitionColumns, partitionValue.columnValues()))
                .distinct()
                .collect(toImmutableList());

        List<String> tableColumnNames = partitionColumns.stream()
                .map(field -> field.column().columnName())
                .collect(toImmutableList());
        List<String> partitionColumnNames = partitionColumns.stream()
                .map(OptimizablePartitionColumn::partitionTransformedColumnName)
                .collect(toImmutableList());

        return new PartitionedBasedMaintenanceDetails(
                new OptimizeQueryIterator(tableName, partitionOptimizeQueries, optimizeFileSizeThreshold),
                new PartitionColumnBasedOptimizeStatus(tableColumnNames, partitionColumnNames, ImmutableList.of()));
    }

    private String buildUnoptimizedPartitionsQuery(CatalogSchemaTableName tableName, List<OptimizablePartitionColumn> partitionColumns)
    {
        String selectClause = IntStream.range(0, partitionColumns.size())
                .mapToObj(i -> {
                    String escapedName = doubleQuoteEscape(partitionColumns.get(i).partitionTransformedColumnName());
                    return "  CAST(partition.\"%s\" AS VARCHAR) as partition_column_value_%d,\n  any_value(typeof(partition.\"%s\")) as partition_column_type_%d"
                            .formatted(escapedName, i, escapedName, i);
                })
                .collect(Collectors.joining(",\n"));

        String groupByClause = partitionColumns.stream()
                .map(field -> "  partition.\"%s\"".formatted(doubleQuoteEscape(field.partitionTransformedColumnName())))
                .collect(Collectors.joining(",\n"));

        return """
               SELECT DISTINCT
               %s,
                 CASE content
                   WHEN 0 THEN 'DATA'
                   WHEN 1 THEN 'POSITION_DELETES'
                   WHEN 2 THEN 'EQUALITY_DELETES'
                 END as file_type
               FROM
                 "%s"."%s"."%s$files"
               WHERE
                 (content = 0 AND file_size_in_bytes < %d)
                 OR content IN (1, 2)
               GROUP BY
               %s,
                 partition,
                 content
               HAVING
                 (content = 0 AND COUNT(*) >= 2)
                 OR content IN (1, 2)
               """.formatted(
                selectClause,
                doubleQuoteEscape(tableName.getCatalogName()),
                doubleQuoteEscape(tableName.getSchemaTableName().getSchemaName()),
                doubleQuoteEscape(tableName.getSchemaTableName().getTableName()),
                optimizeFileSizeThreshold.toBytes(),
                groupByClause);
    }

    private static String buildWherePredicate(List<OptimizablePartitionColumn> partitionColumns, List<ColumnValue> columnValues)
    {
        checkArgument(partitionColumns.size() == columnValues.size(),
                "partitionColumns and columnValues must have the same size, got %s and %s",
                partitionColumns.size(),
                columnValues.size());
        return Streams.zip(
                        partitionColumns.stream()
                                .map(OptimizablePartitionColumn::partitionComparableColumnValue),
                        columnValues.stream()
                                .map(ColumnValue::getQueryValue),
                        "%s = %s"::formatted)
                .collect(Collectors.joining(" AND ", "WHERE ", ""));
    }

    private static List<OptimizablePartitionColumn> findOptimizablePartitionColumns(List<TableColumn> columns, List<String> partitionEntries)
    {
        Map<String, TableColumn> columnNameToColumn = columns.stream()
                .collect(toImmutableMap(TableColumn::columnName, identity()));
        ImmutableList.Builder<OptimizablePartitionColumn> result = ImmutableList.builder();
        for (String partitionEntry : partitionEntries) {
            Transform.resolveTransformedPartitionColumn(partitionEntry)
                    .flatMap(classified -> Optional.ofNullable(columnNameToColumn.get(classified.sourceColumn()))
                            .map(column -> new OptimizablePartitionColumn(column, classified.transform())))
                    .ifPresent(result::add);
        }
        return result.build();
    }

    private List<String> computeTablePartitionColumns(CatalogSchemaTableName tableName)
    {
        String showCreateTableQuery = SHOW_CREATE_TABLE_QUERY.formatted(
                doubleQuoteEscape(tableName.getCatalogName()),
                doubleQuoteEscape(tableName.getSchemaTableName().getSchemaName()),
                doubleQuoteEscape(tableName.getSchemaTableName().getTableName()));
        String showCreateTableOutput = determinePartitionOptimizeQueryRunner.runShowCreateTableQuery(showCreateTableQuery);

        String arrayVarcharColumns = null;
        Matcher arrayVarcharColumnsMatcher = PARTITIONING_PROPERTY_EXTRACT_PATTERN.matcher(showCreateTableOutput);
        if (arrayVarcharColumnsMatcher.find()) {
            arrayVarcharColumns = arrayVarcharColumnsMatcher.group(1);
        }
        if (arrayVarcharColumns == null || arrayVarcharColumns.isEmpty()) {
            return ImmutableList.of();
        }
        ImmutableList.Builder<String> result = ImmutableList.builder();
        Matcher entryMatcher = PARTITIONING_ENTRIES_EXTRACT_PATTERN.matcher(arrayVarcharColumns);
        while (entryMatcher.find()) {
            result.add(entryMatcher.group(1));
        }
        return result.build();
    }

    private List<TableColumn> computeTableColumnNames(CatalogSchemaTableName tableName)
    {
        String getColumnNamesQuery = GET_COLUMN_NAMES_QUERY.formatted(
                doubleQuoteEscape(tableName.getCatalogName()),
                singleQuoteEscape(tableName.getCatalogName()),
                singleQuoteEscape(tableName.getSchemaTableName().getSchemaName()),
                singleQuoteEscape(tableName.getSchemaTableName().getTableName()));
        return determinePartitionOptimizeQueryRunner.runGetColumnsQuery(getColumnNamesQuery);
    }
}
