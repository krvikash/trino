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

import com.google.inject.Inject;
import io.trino.plugin.jdbc.DefaultQueryBuilder;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.QueryParameter;
import io.trino.plugin.jdbc.WriteFunction;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.CharType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.util.Optional;
import java.util.function.Consumer;

import static io.trino.plugin.singlestore.SingleStoreSessionProperties.isEnableStringPushdownWithBinary;

public class BinaryComparisonQueryBuilder
        extends DefaultQueryBuilder
{
    @Inject
    public BinaryComparisonQueryBuilder(RemoteQueryModifier queryModifier)
    {
        super(queryModifier);
    }

    @Override
    protected String toPredicate(
            JdbcClient client,
            ConnectorSession session,
            JdbcColumnHandle column,
            JdbcTypeHandle jdbcType,
            Type type,
            WriteFunction writeFunction,
            String operator,
            Object value,
            Consumer<QueryParameter> accumulator)
    {
        if (isStringType(column) && isEnableStringPushdownWithBinary(session)) {
            accumulator.accept(new QueryParameter(jdbcType, type, Optional.of(value)));
            return "BINARY %s %s %s".formatted(client.quoted(column.getColumnName()), operator, writeFunction.getBindExpression());
        }

        return super.toPredicate(client, session, column, jdbcType, type, writeFunction, operator, value, accumulator);
    }

    private static boolean isStringType(JdbcColumnHandle column)
    {
        return column.getColumnType() instanceof CharType || column.getColumnType() instanceof VarcharType;
    }
}
