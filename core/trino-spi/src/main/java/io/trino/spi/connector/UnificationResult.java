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
package io.trino.spi.connector;

import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.predicate.TupleDomain;

import java.util.Map;
import java.util.OptionalLong;

import static io.trino.spi.expression.Constant.TRUE;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

/**
 * A result of unifying two table handles.
 *
 * @param unifiedHandle -- a handle that satisfies semantics of the first and second unified handles
 * @param firstCompensationFilter -- previously enforced filter of the first handle. If applied together with {@code firstCompensationExpression} over the unifiedHandle, it will restore the semantics of the first handle
 * @param firstCompensationExpression -- previously enforced filter of the first handle. If applied together with {@code firstCompensationFilter} over the unifiedHandle, it will restore the semantics of the first handle
 * @param firstAssignments -- assignments corresponding to {@code firstCompensationExpression}
 * @param secondCompensationFilter -- previously enforced filter of the second handle. If applied together with {@code secondCompensationExpression} over the unifiedHandle, it will restore the semantics of the second handle
 * @param secondCompensationExpression -- previously enforced expression of the second handle. If applied together with {@code secondCompensationFilter} over the unifiedHandle, it will restore the semantics of the second handle
 * @param secondAssignments -- assignments corresponding to {@code secondCompensationExpression}
 * @param enforcedProperties -- predicate and limit guaranteed by the unifiedHandle
 */
public record UnificationResult<T>(
        T unifiedHandle,
        TupleDomain<ColumnHandle> firstCompensationFilter,
        ConnectorExpression firstCompensationExpression,
        Map<String, Assignment> firstAssignments,
        TupleDomain<ColumnHandle> secondCompensationFilter,
        ConnectorExpression secondCompensationExpression,
        Map<String, Assignment> secondAssignments,
        Properties enforcedProperties)
{
    public UnificationResult
    {
        requireNonNull(unifiedHandle, "unifiedHandle is null");
        requireNonNull(firstCompensationFilter, "firstCompensationFilter is null");
        requireNonNull(firstCompensationExpression, "firstCompensationExpression is null");
        requireNonNull(firstAssignments, "firstAssignments is null");
        requireNonNull(secondCompensationFilter, "secondCompensationFilter is null");
        requireNonNull(secondCompensationExpression, "secondCompensationExpression is null");
        requireNonNull(secondAssignments, "secondAssignments is null");
        requireNonNull(enforcedProperties, "enforcedProperties is null");
    }

    public UnificationResult(T unifiedHandle, TupleDomain<ColumnHandle> firstCompensationFilter, TupleDomain<ColumnHandle> secondCompensationFilter, Properties enforcedProperties)
    {
        this(unifiedHandle, firstCompensationFilter, TRUE, emptyMap(), secondCompensationFilter, TRUE, emptyMap(), enforcedProperties);
    }

    public record Properties(
            TupleDomain<ColumnHandle> tupleDomainConstraint,
            ConnectorExpression connectorExpressionConstraint,
            Map<String, Assignment> connectorExpressionAssignments,
            OptionalLong limit)
    {
        public Properties
        {
            requireNonNull(tupleDomainConstraint, "tupleDomainConstraint is null");
            requireNonNull(connectorExpressionConstraint, "connectorExpressionConstraint is null");
            requireNonNull(connectorExpressionAssignments, "connectorExpressionAssignments is null");
            requireNonNull(limit, "limit is null");
        }
    }
}
