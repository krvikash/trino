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
package io.trino.plugin.sas;

import com.epam.parso.Column;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

public record SasColumn(
        String name,
        Type type)
{
    public SasColumn
    {
        checkArgument(!isNullOrEmpty(name), "name is null or is empty");
        requireNonNull(type, "type is null");
    }

    public static SasColumn getColumn(Column column)
    {
        String name = column.getName();
        if ("java.lang.String".equals(column.getType().getName())) {
            int length = column.getLength();
            return new SasColumn(
                    name,
                    length > 0 ? VarcharType.createVarcharType(length) : VarcharType.createUnboundedVarcharType());
        }
        if ("java.lang.Number".equals(column.getType().getName())) {
            if (column.getFormat().getName() == null || column.getFormat().getName().isEmpty()) {
                return new SasColumn(name, DoubleType.DOUBLE);
            }
            return switch (column.getFormat().getName()) {
                case "HOUR", "MMSS", "HHMM" -> new SasColumn(name, IntegerType.INTEGER);
                case "PERCENT" -> new SasColumn(name, DoubleType.DOUBLE);
                case "E8601TM", "E8601LS" -> new SasColumn(name, VarcharType.createUnboundedVarcharType());
                case "MMDDYY", "DDMMYYD" -> new SasColumn(name, DateType.DATE);
                default -> {
                    if (column.getFormat().getName().contains("TIME")) {
                        yield new SasColumn(name, TimestampType.TIMESTAMP_MILLIS);
                    }
                    yield new SasColumn(name, VarcharType.createUnboundedVarcharType());
                }
            };
        }
        return new SasColumn(name, VarcharType.createUnboundedVarcharType());
    }
}
