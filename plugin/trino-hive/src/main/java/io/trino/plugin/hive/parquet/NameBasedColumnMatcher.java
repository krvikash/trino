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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Case-insensitive name-based column matching for Hive tables.
 */
public class NameBasedColumnMatcher
        implements ColumnMatchingStrategy
{
    @Override
    public Optional<Type> findColumn(HiveColumnHandle column, MessageType schema)
    {
        requireNonNull(column, "column is null");
        requireNonNull(schema, "schema is null");

        String columnName = column.getBaseColumnName();
        for (Type field : schema.getFields()) {
            if (field.getName().equalsIgnoreCase(columnName)) {
                return Optional.of(field);
            }
        }
        return Optional.empty();
    }

    @Override
    public MessageType clipSchema(MessageType fullSchema, List<HiveColumnHandle> columns)
    {
        requireNonNull(fullSchema, "fullSchema is null");
        requireNonNull(columns, "columns is null");

        List<Type> clippedFields = new ArrayList<>();
        for (HiveColumnHandle column : columns) {
            findColumn(column, fullSchema).ifPresent(clippedFields::add);
        }

        return new MessageType(fullSchema.getName(), clippedFields);
    }
}
