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

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.starburst.stargate.tablemaintenance.MaintenanceQueryGenerators.doubleQuoteEscape;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;

public enum Transform
{
    IDENTITY(Optional.empty(), "", "\"%s\""),
    DAY(Optional.of("day"), "_day", "date(\"%s\")");

    private static final List<Transform> PREFIXED_FIRST = Arrays.stream(Transform.values())
            .sorted(comparing((Transform t) -> t.transformPrefix.isEmpty()))
            .collect(toImmutableList());

    private final Optional<String> transformPrefix;
    private final String partitionTransformedColumnSuffix;
    private final String tableColumnValueModificationTemplate;

    Transform(Optional<String> transformPrefix, String partitionTransformedColumnSuffix, String tableColumnValueModificationTemplate)
    {
        this.transformPrefix = requireNonNull(transformPrefix, "transformPrefix is null");
        this.partitionTransformedColumnSuffix = requireNonNull(partitionTransformedColumnSuffix, "partitionTransformedColumnSuffix is null");
        this.tableColumnValueModificationTemplate = requireNonNull(tableColumnValueModificationTemplate, "tableColumnValueModificationTemplate is null");
    }

    String toPartitionTransformedColumnName(String sourceColumn)
    {
        return sourceColumn + partitionTransformedColumnSuffix;
    }

    public String toPartitionComparableColumnValue(String sourceColumn)
    {
        return tableColumnValueModificationTemplate.formatted(doubleQuoteEscape(sourceColumn));
    }

    public static Optional<Transform> findApplicableTransform(String sourceColumn, String partitionTransformedColumnName)
    {
        return PREFIXED_FIRST.stream()
                .filter(transform -> transform.toPartitionTransformedColumnName(sourceColumn).equals(partitionTransformedColumnName))
                .findFirst();
    }

    private Optional<TransformedPartitionColumn> asTransformedPartitionColumn(String sourcePartitionEntry)
    {
        if (transformPrefix.isEmpty()) {
            return Optional.of(new TransformedPartitionColumn(sourcePartitionEntry, this));
        }
        if (sourcePartitionEntry.startsWith(transformPrefix.get() + "(")) {
            // e.g. day(event_date) -> event_date
            String rawColumnName = sourcePartitionEntry.substring(transformPrefix.get().length() + 1, sourcePartitionEntry.length() - 1);
            return Optional.of(new TransformedPartitionColumn(rawColumnName, this));
        }
        return Optional.empty();
    }

    static Optional<TransformedPartitionColumn> resolveTransformedPartitionColumn(String partitionEntry)
    {
        return PREFIXED_FIRST.stream()
                .flatMap(transform -> transform.asTransformedPartitionColumn(partitionEntry).stream())
                .findFirst();
    }

    public record TransformedPartitionColumn(String sourceColumn, Transform transform)
    {
        public TransformedPartitionColumn
        {
            requireNonNull(sourceColumn, "sourceColumn is null");
            requireNonNull(transform, "transform is null");
        }
    }
}
