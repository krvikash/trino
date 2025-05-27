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
package io.trino.plugin.hive.metastore.unity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static java.util.Objects.requireNonNull;

public record Metadata(
        @JsonProperty("delta_table_id") String deltaTableId,
        @JsonProperty("name") String name,
        @JsonProperty("description") String description,
        @JsonProperty("partition_columns") List<String> partitionColumns,
        @JsonProperty("create_time") String createTime)
{
    @JsonCreator
    public Metadata
    {
        requireNonNull(deltaTableId, "deltaTableId is null");
        requireNonNull(name, "name is null");
        requireNonNull(description, "description is null");
        partitionColumns = ImmutableList.copyOf(partitionColumns);
        requireNonNull(createTime, "createTime is null");
    }
}
