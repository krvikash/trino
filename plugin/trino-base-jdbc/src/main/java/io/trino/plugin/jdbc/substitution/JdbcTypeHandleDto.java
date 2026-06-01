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
package io.trino.plugin.jdbc.substitution;

import io.trino.plugin.jdbc.CaseSensitivity;
import io.trino.plugin.jdbc.JdbcTypeHandle;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Persisted copy of a {@link JdbcTypeHandle} embedded in {@link JdbcColumnId}. Kept separate from
 * {@code JdbcTypeHandle} so the substitution column identity can evolve independently of the live
 * connector type handle.
 */
public record JdbcTypeHandleDto(
        int jdbcType,
        Optional<String> jdbcTypeName,
        Optional<Integer> columnSize,
        Optional<Integer> decimalDigits,
        Optional<Integer> arrayDimensions,
        Optional<CaseSensitivity> caseSensitivity)
{
    public JdbcTypeHandleDto
    {
        requireNonNull(jdbcTypeName, "jdbcTypeName is null");
        requireNonNull(columnSize, "columnSize is null");
        requireNonNull(decimalDigits, "decimalDigits is null");
        requireNonNull(arrayDimensions, "arrayDimensions is null");
        requireNonNull(caseSensitivity, "caseSensitivity is null");
    }

    public static JdbcTypeHandleDto from(JdbcTypeHandle typeHandle)
    {
        return new JdbcTypeHandleDto(
                typeHandle.jdbcType(),
                typeHandle.jdbcTypeName(),
                typeHandle.columnSize(),
                typeHandle.decimalDigits(),
                typeHandle.arrayDimensions(),
                typeHandle.caseSensitivity());
    }
}
