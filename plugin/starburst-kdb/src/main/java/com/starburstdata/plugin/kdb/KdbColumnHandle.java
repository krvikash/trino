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
package com.starburstdata.plugin.kdb;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.type.Type;

import static java.util.Objects.requireNonNull;

public record KdbColumnHandle(String columnName, Type columnType, char kdbType, int ordinalPosition)
        implements ColumnHandle
{
    public KdbColumnHandle
    {
        requireNonNull(columnName, "columnName is null");
        requireNonNull(columnType, "columnType is null");
    }

    @Override
    public String toString()
    {
        return "%s:%s(%s)".formatted(columnName, columnType, kdbType);
    }
}
