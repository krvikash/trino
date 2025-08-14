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
package io.trino.plugin.base.util;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.expression.ConnectorExpression;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public final class ConnectorExpressionUtil
{
    private ConnectorExpressionUtil() {}

    public record ExpressionAndAssignments(ConnectorExpression expression, Map<String, ColumnHandle> assignments)
    {
        public ExpressionAndAssignments
        {
            requireNonNull(expression, "expression is null");
            assignments = ImmutableMap.copyOf(requireNonNull(assignments, "assignments is null"));
        }
    }
}
