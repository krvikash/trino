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
package io.trino.plugin.oracle;

import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.expression.AbstractRewriteCast;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.CharType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import oracle.jdbc.OracleTypes;

import java.util.Optional;
import java.util.function.BiFunction;

import static io.trino.plugin.oracle.OracleClient.ORACLE_CHAR_MAX_CHARS;
import static io.trino.plugin.oracle.OracleClient.ORACLE_VARCHAR2_MAX_CHARS;

public class RewriteCast
        extends AbstractRewriteCast
{
    public RewriteCast(BiFunction<ConnectorSession, Type, String> jdbcTypeProvider)
    {
        super(jdbcTypeProvider);
    }

    @Override
    protected Optional<JdbcTypeHandle> toJdbcTypeHandle(JdbcTypeHandle sourceType, Type targetType)
    {
        if (!pushdownSupported(sourceType, targetType)) {
            return Optional.empty();
        }

        if (targetType instanceof VarcharType varcharType) {
            return Optional.of(new JdbcTypeHandle(OracleTypes.VARCHAR, Optional.of(varcharType.getBaseName()), varcharType.getLength(), Optional.empty(), Optional.empty(), Optional.empty()));
        }
        return Optional.empty();
    }

    private boolean pushdownSupported(JdbcTypeHandle sourceType, Type targetType)
    {
        if (targetType instanceof VarcharType varcharType && !varcharType.isUnbounded()) {
            // unbounded varchar and char(n>ORACLE_VARCHAR2_MAX_CHARS) gets written as nclob.
            // pushdown does not happen when comparing nclob type variable, so skipping to pushdown the cast for nclob type variable.
            return varcharType.getLength().orElseThrow() <= ORACLE_VARCHAR2_MAX_CHARS
                    && supportedSourceTypeToCastToVarchar(sourceType);
        }
        return false;
    }

    private static boolean supportedSourceTypeToCastToVarchar(JdbcTypeHandle sourceType)
    {
        return switch (sourceType.jdbcType()) {
            case OracleTypes.NUMBER,
                    OracleTypes.CHAR,
                    OracleTypes.VARCHAR,
                    OracleTypes.NCHAR,
                    OracleTypes.NVARCHAR,
                    OracleTypes.CLOB,
                    OracleTypes.NCLOB -> true;
            default -> false;
        };
    }
}
