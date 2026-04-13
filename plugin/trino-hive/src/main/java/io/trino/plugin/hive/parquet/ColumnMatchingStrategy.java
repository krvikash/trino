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
package io.trino.plugin.hive.parquet;

import io.trino.plugin.hive.HiveColumnHandle;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.util.List;
import java.util.Optional;

/**
 * Strategy for matching Hive columns to Parquet schema fields.
 * Different table formats (Hive, Iceberg) use different matching strategies.
 */
public interface ColumnMatchingStrategy
{
    /**
     * Find the Parquet field that corresponds to the given Hive column.
     *
     * @param column the Hive column to find
     * @param schema the Parquet file schema
     * @return the matching Parquet field, or empty if not found
     */
    Optional<Type> findColumn(HiveColumnHandle column, MessageType schema);

    /**
     * Create a clipped schema containing only the requested columns.
     * The returned schema will contain only the fields that match the requested columns.
     *
     * @param fullSchema the full Parquet file schema
     * @param columns the columns to include
     * @return a new MessageType with only the requested columns
     */
    MessageType clipSchema(MessageType fullSchema, List<HiveColumnHandle> columns);
}
