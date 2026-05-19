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
package io.trino.plugin.warp.dispatcher;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.plugin.base.util.ConnectorExpressionUtil.ExpressionAndAssignments;
import io.trino.plugin.warp.expression.rewrite.WarpExpression;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class DispatcherTableHandle
        implements ConnectorTableHandle
{
    public static final String PARTITION_NULL_VALUE_STR = "\\N";

    private final SchemaTableName schemaTableName;
    private final OptionalLong limit;
    private final TupleDomain<ColumnHandle> fullPredicate;
    private final SimplifiedColumns simplifiedColumns;
    private final ConnectorTableHandle proxyConnectorTableHandle;
    private final Optional<WarpExpression> warpExpression;
    private final List<CustomStat> customStats;
    private final boolean subsumedPredicates;
    private final Set<String> columnsNotFitForDictionary;

    // used for re-constructing warpExpression
    private final Optional<ExpressionAndAssignments> originalExpression;

    private final FilteringStats filteringStats;

    @JsonCreator
    public DispatcherTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("limit") OptionalLong limit,
            @JsonProperty("fullPredicate") TupleDomain<ColumnHandle> fullPredicate,
            @JsonProperty("simplifiedColumns") SimplifiedColumns simplifiedColumns,
            @JsonProperty("proxyConnectorTableHandle") ConnectorTableHandle proxyConnectorTableHandle,
            @JsonProperty("warpExpression") Optional<WarpExpression> warpExpression,
            @JsonProperty("customStats") List<CustomStat> customStats,
            @JsonProperty("subsumedPredicates") boolean subsumedPredicates,
            @JsonProperty("columnsNotFitForDictionary") Set<String> columnsNotFitForDictionary)
    {
        this(schemaName,
                tableName,
                limit,
                fullPredicate,
                simplifiedColumns,
                proxyConnectorTableHandle,
                warpExpression,
                customStats,
                subsumedPredicates,
                columnsNotFitForDictionary,
                Optional.empty()); // constraintOriginalExpressions is not serialized (not needed in workers)
    }

    public DispatcherTableHandle(
            String schemaName,
            String tableName,
            OptionalLong limit,
            TupleDomain<ColumnHandle> fullPredicate,
            SimplifiedColumns simplifiedColumns,
            ConnectorTableHandle proxyConnectorTableHandle,
            Optional<WarpExpression> warpExpression,
            List<CustomStat> customStats,
            boolean subsumedPredicates,
            Set<String> columnsNotFitForDictionary,
            Optional<ExpressionAndAssignments> originalExpression)
    {
        this.schemaTableName = new SchemaTableName(requireNonNull(schemaName), requireNonNull(tableName));
        this.limit = limit;
        this.fullPredicate = requireNonNull(fullPredicate);
        this.simplifiedColumns = requireNonNull(simplifiedColumns);
        this.proxyConnectorTableHandle = requireNonNull(proxyConnectorTableHandle);
        this.warpExpression = requireNonNull(warpExpression);
        this.customStats = customStats;
        this.subsumedPredicates = subsumedPredicates;
        this.columnsNotFitForDictionary = columnsNotFitForDictionary;
        this.originalExpression = requireNonNull(originalExpression);

        this.filteringStats = new FilteringStats();
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaTableName.getSchemaName();
    }

    @JsonProperty
    public String getTableName()
    {
        return schemaTableName.getTableName();
    }

    @JsonIgnore
    public static Optional<String> getNullPartitionValue()
    {
        // from HivePartitionKey
        return Optional.of(PARTITION_NULL_VALUE_STR);
    }

    @JsonProperty
    public List<CustomStat> getCustomStats()
    {
        return customStats;
    }

    @JsonProperty
    public boolean isSubsumedPredicates()
    {
        return subsumedPredicates;
    }

    @JsonProperty
    public OptionalLong getLimit()
    {
        return limit;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getFullPredicate()
    {
        return fullPredicate;
    }

    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
    }

    @JsonProperty
    public SimplifiedColumns getSimplifiedColumns()
    {
        return simplifiedColumns;
    }

    @JsonProperty
    public ConnectorTableHandle getProxyConnectorTableHandle()
    {
        return proxyConnectorTableHandle;
    }

    @JsonProperty
    public Optional<WarpExpression> getWarpExpression()
    {
        return warpExpression;
    }

    @JsonProperty
    public Set<String> getColumnsNotFitForDictionary()
    {
        return columnsNotFitForDictionary;
    }

    public boolean isColumnFitForDictionary(String columnName)
    {
        return columnsNotFitForDictionary.isEmpty() || !columnsNotFitForDictionary.contains(columnName);
    }

    @JsonIgnore
    public Optional<ExpressionAndAssignments> getOriginalExpression()
    {
        return originalExpression;
    }

    @JsonIgnore
    public FilteringStats getFilteringStats()
    {
        return filteringStats;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DispatcherTableHandle that = (DispatcherTableHandle) o;
        return subsumedPredicates == that.subsumedPredicates &&
                Objects.equals(schemaTableName, that.schemaTableName) &&
                Objects.equals(limit, that.limit) &&
                Objects.equals(fullPredicate, that.fullPredicate) &&
                Objects.equals(simplifiedColumns, that.simplifiedColumns) &&
                Objects.equals(warpExpression, that.warpExpression) &&
                Objects.equals(proxyConnectorTableHandle, that.proxyConnectorTableHandle) &&
                Objects.equals(columnsNotFitForDictionary, that.columnsNotFitForDictionary);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(
                schemaTableName,
                limit,
                fullPredicate,
                simplifiedColumns,
                proxyConnectorTableHandle,
                warpExpression,
                subsumedPredicates,
                columnsNotFitForDictionary);
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append("proxyConnectorTableHandle=").append(proxyConnectorTableHandle);
        builder.append(", fullPredicate=").append(fullPredicate);
        builder.append(", simplifiedColumns=").append(simplifiedColumns);
        builder.append(", subsumedPredicates=").append(subsumedPredicates);
        if (!columnsNotFitForDictionary.isEmpty()) {
            builder.append(", columnsNotFitForDictionary=").append(columnsNotFitForDictionary);
        }
        limit.ifPresent(value -> builder.append(", limit=").append(value));
        warpExpression.ifPresent(value -> builder.append(", warpExpression=").append(value));
        return builder.toString();
    }
}
