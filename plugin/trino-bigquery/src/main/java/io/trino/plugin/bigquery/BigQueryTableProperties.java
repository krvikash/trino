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
package io.trino.plugin.bigquery;

import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.common.collect.ImmutableList;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.session.PropertyMetadata.stringProperty;

public class BigQueryTableProperties
{
    public static final String PARTITIONED_BY_PROPERTY = "partitioned_by";

    private final List<PropertyMetadata<?>> tableProperties;

    public BigQueryTableProperties()
    {
        tableProperties = ImmutableList.<PropertyMetadata<?>>builder()
                .add(stringProperty(
                        PARTITIONED_BY_PROPERTY,
                        "Partition column",
                        null,
                        false))
                .build();
    }

    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }

    public static Optional<String> getPartitionedBy(Map<String, Object> properties)
    {
        return Optional.ofNullable((String) properties.get(PARTITIONED_BY_PROPERTY));
    }

    public static Optional<String> getPartitionColumn(TableDefinition tableDefinition)
    {
        if (tableDefinition instanceof StandardTableDefinition definition) {
            TimePartitioning timePartition = definition.getTimePartitioning();
            return Optional.ofNullable(timePartition).map(TimePartitioning::getField);
        }
        return Optional.empty();
    }
}
