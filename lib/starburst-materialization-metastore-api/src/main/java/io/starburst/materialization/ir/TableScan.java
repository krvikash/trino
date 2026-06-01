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
package io.starburst.materialization.ir;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.util.StdConverter;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.connector.substitution.ConnectorColumnId;

import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

@JsonTypeName(TableScan.NAME)
public record TableScan(
        TableId table,
        @JsonSerialize(converter = AssignmentsToList.class)
        @JsonDeserialize(converter = ListToAssignments.class)
        Map<ConnectorColumnId, Symbol> assignments)
        implements Operation
{
    static final String NAME = "TableScan";
    static final int VERSION = 1;

    public TableScan
    {
        requireNonNull(table, "table is null");
        assignments = ImmutableMap.copyOf(requireNonNull(assignments, "assignments is null"));
    }

    @Override
    public int version()
    {
        return VERSION;
    }

    @Override
    public String name()
    {
        return NAME;
    }

    public record Assignment(ConnectorColumnId id, Symbol symbol)
    {
        @JsonCreator
        public Assignment(
                @JsonProperty("id") ConnectorColumnId id,
                @JsonProperty("symbol") Symbol symbol)
        {
            this.id = requireNonNull(id, "id is null");
            this.symbol = requireNonNull(symbol, "symbol is null");
        }
    }

    /**
     * Serializes the assignments map as a JSON array of {@link Assignment} pairs. JSON map keys
     * must be strings, so we cannot directly serialize a {@code Map<ConnectorColumnId, ...>}
     * whose key is a polymorphic SPI type — encode as a list of pairs instead.
     */
    public static final class AssignmentsToList
            extends StdConverter<Map<ConnectorColumnId, Symbol>, List<Assignment>>
    {
        @Override
        public List<Assignment> convert(Map<ConnectorColumnId, Symbol> value)
        {
            return value.entrySet().stream()
                    .map(entry -> new Assignment(entry.getKey(), entry.getValue()))
                    .collect(toImmutableList());
        }
    }

    public static final class ListToAssignments
            extends StdConverter<List<Assignment>, Map<ConnectorColumnId, Symbol>>
    {
        @Override
        public Map<ConnectorColumnId, Symbol> convert(List<Assignment> value)
        {
            return value.stream()
                    .collect(toImmutableMap(Assignment::id, Assignment::symbol));
        }
    }
}
