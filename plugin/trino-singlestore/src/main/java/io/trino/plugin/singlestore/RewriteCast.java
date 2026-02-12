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
package io.trino.plugin.singlestore;

import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.expression.AbstractRewriteCast;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.util.Optional;
import java.util.function.BiFunction;

import static java.sql.Types.LONGVARCHAR;
import static java.sql.Types.VARCHAR;

class RewriteCast
        extends AbstractRewriteCast
{
    // For varchar casting SingleStore allows to use CAST(column as CHAR(N)/CHAR) syntax.
    // It behaves consistently with trino cast in context of truncation at least for varchar types but allows max N = 8192.
    // For N > 8192 we have to use SUBSTRING function when truncation is needed.
    private static final int MAX_CHAR_CAST_SIZE = 8192;

    RewriteCast(BiFunction<ConnectorSession, Type, String> jdbcTypeProvider)
    {
        super(jdbcTypeProvider);
    }

    @Override
    protected Optional<JdbcTypeHandle> toJdbcTypeHandle(
            ConnectorSession session,
            JdbcTypeHandle sourceTypeHandle,
            Type sourceType,
            Type targetType)
    {
        return switch (targetType) {
            case VarcharType targetVarcharType -> handleForVarcharCast(sourceType, targetVarcharType);
            default -> Optional.empty();
        };
    }

    private static Optional<JdbcTypeHandle> handleForVarcharCast(Type sourceType, VarcharType targetVarcharType)
    {
        if (sourceType instanceof VarcharType) {
            if (targetVarcharType.isUnbounded()) {
                return Optional.of(new JdbcTypeHandle(LONGVARCHAR, Optional.of("LONGTEXT"), Optional.of(Integer.MAX_VALUE), Optional.empty(), Optional.empty(), Optional.empty()));
            }
            return Optional.of(new JdbcTypeHandle(VARCHAR, Optional.of("VARCHAR"), Optional.of(targetVarcharType.getBoundedLength()), Optional.empty(), Optional.empty(), Optional.empty()));
        }
        return Optional.empty();
    }

    @Override
    protected String buildCast(ConnectorSession session, JdbcTypeHandle sourceTypeJdbcHandle, Type sourceType, Type targetType, String expression, String castType)
    {
        return switch (targetType) {
            case VarcharType varcharTargetType -> buildCastToVarchar((VarcharType) sourceType, varcharTargetType, expression);
            default -> throw new IllegalArgumentException("Casting to target data type is not supported: " + targetType.getDisplayName());
        };
    }

    private static String buildCastToVarchar(VarcharType sourceType, VarcharType targetType, String expression)
    {
        if (targetType.isUnbounded()) {
            return "CAST(%s AS CHAR)".formatted(expression);
        }
        int targetLength = targetType.getBoundedLength();
        if (isSourceVarcharSmallerThanTargetVarchar(sourceType, targetLength)) {
            // no truncation needed
            return expression;
        }
        if (targetLength <= MAX_CHAR_CAST_SIZE) {
            return "CAST(%s AS CHAR(%d))".formatted(expression, targetLength);
        }
        return "SUBSTRING(%s, 1, %s)".formatted(expression, targetLength);
    }

    private static boolean isSourceVarcharSmallerThanTargetVarchar(VarcharType sourceType, int targetLength)
    {
        return sourceType.getLength().map(sourceLength -> sourceLength <= targetLength).orElse(false);
    }
}
