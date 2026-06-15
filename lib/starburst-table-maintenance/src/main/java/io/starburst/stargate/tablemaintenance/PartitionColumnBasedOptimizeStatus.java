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
package io.starburst.stargate.tablemaintenance;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.ImmutableList;
import io.starburst.stargate.tablemaintenance.partitioned.Transform;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.starburst.stargate.tablemaintenance.partitioned.Transform.findApplicableTransform;
import static java.util.Objects.requireNonNull;

@JsonTypeName("partitionColumnBased")
public record PartitionColumnBasedOptimizeStatus(
        // taken from information schema and are literal table columns involved in partitioning
        List<String> tableColumns,
        // taken from partitioning property and represent partition columns, in case of identity partitions, they are equal with tableColumns
        List<String> partitionColumns,
        List<String> optimizedInQueryPartitionValues)
        implements TableOptimizeStatus
{
    public PartitionColumnBasedOptimizeStatus
    {
        partitionColumns = ImmutableList.copyOf(requireNonNull(partitionColumns, "partitionColumns is null"));
        checkArgument(!partitionColumns.isEmpty(), "partitionColumns must not be empty");
        tableColumns = ImmutableList.copyOf(requireNonNull(tableColumns, "tableColumns is null"));
        checkArgument(!tableColumns.isEmpty(), "tableColumns must not be empty");
        checkArgument(tableColumns.size() == partitionColumns.size(),
                "tableColumns and partitionColumns must be same size; got tableColumns=%s and partitionColumns=%s",
                tableColumns,
                partitionColumns);
        optimizedInQueryPartitionValues = ImmutableList.copyOf(optimizedInQueryPartitionValues);
    }

    public PartitionColumnBasedOptimizeStatus(List<String> partitionColumns, List<String> optimizedInQueryPartitionValues)
    {
        this(partitionColumns, partitionColumns, optimizedInQueryPartitionValues);
    }

    /**
     * Deserializes old format (with {@code tableColumn}/{@code partitionColumn} single fields)
     * and new format with lists
     */
    @JsonCreator
    public static PartitionColumnBasedOptimizeStatus fromJson(
            @JsonProperty("partitionColumn") String partitionColumn,
            @JsonProperty("tableColumn") String tableColumn,
            @JsonProperty("tableColumns") List<String> tableColumns,
            @JsonProperty("partitionColumns") List<String> partitionColumns,
            @JsonProperty("optimizedInQueryPartitionValues") List<String> optimizedInQueryPartitionValues)
    {
        if (partitionColumn != null && tableColumn != null) {
            return new PartitionColumnBasedOptimizeStatus(
                    ImmutableList.of(tableColumn),
                    ImmutableList.of(partitionColumn),
                    optimizedInQueryPartitionValues);
        }
        return new PartitionColumnBasedOptimizeStatus(
                tableColumns,
                partitionColumns,
                optimizedInQueryPartitionValues);
    }

    @Override
    public TableOptimizeStatus withLatestOptimizeQuery(String optimizeQuery)
    {
        if (optimizeQuery.contains("WHERE")) {
            String afterWhere = optimizeQuery.substring(optimizeQuery.indexOf("WHERE") + "WHERE".length());
            String partitionValuePredicate = afterWhere.strip();
            for (int i = 0; i < partitionColumns.size(); i++) {
                partitionValuePredicate = removeLeftSideOfEqualityCheck(tableColumns.get(i), partitionColumns.get(i), partitionValuePredicate);
            }
            return new PartitionColumnBasedOptimizeStatus(
                    tableColumns,
                    partitionColumns,
                    ImmutableList.<String>builder()
                            .addAll(optimizedInQueryPartitionValues)
                            .add(partitionValuePredicate)
                            .build());
        }
        return this;
    }

    private static String removeLeftSideOfEqualityCheck(String tableColumn, String partitionColumn, String partitionValuePredicate)
    {
        Transform transform = findApplicableTransform(tableColumn, partitionColumn)
                .orElseThrow(() -> new IllegalArgumentException(
                        "No supported transform produces partition column '%s' from source column '%s'".formatted(partitionColumn, tableColumn)));
        String columnExpression = transform.toPartitionComparableColumnValue(tableColumn);
        int columnExpressionStart = partitionValuePredicate.indexOf(columnExpression + " = ");
        if (columnExpressionStart < 0) {
            return partitionValuePredicate;
        }
        int valueStart = columnExpressionStart + columnExpression.length() + " = ".length();
        return partitionValuePredicate.substring(0, columnExpressionStart) + partitionValuePredicate.substring(valueStart);
    }
}
