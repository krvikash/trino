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
package io.trino.plugin.warp.dispatcher;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.trino.plugin.base.util.ConnectorExpressionUtil.ExpressionAndAssignments;
import io.trino.plugin.warp.expression.rewrite.WarpExpression;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.predicate.TupleDomain;

import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static java.util.Objects.requireNonNull;

@Singleton
public class DispatcherTableHandleBuilderProvider
{
    private final DispatcherProxiedConnectorTransformer transformer;

    @Inject
    public DispatcherTableHandleBuilderProvider(DispatcherProxiedConnectorTransformer transformer)
    {
        this.transformer = requireNonNull(transformer);
    }

    public Builder builder(DispatcherTableHandle dispatcherTableHandle, int predicateThreshold)
    {
        Builder builder = new Builder(transformer, predicateThreshold)
                .proxiedConnectorTableHandle(dispatcherTableHandle.getProxyConnectorTableHandle())
                .schemaName(dispatcherTableHandle.getSchemaName())
                .tableName(dispatcherTableHandle.getTableName())
                .fullPredicate(dispatcherTableHandle.getFullPredicate())
                .warpExpression(dispatcherTableHandle.getWarpExpression())
                .metrics(dispatcherTableHandle.getMetrics())
                .subsumedPredicates(dispatcherTableHandle.isSubsumedPredicates())
                .columnsNotFitForDictionary(dispatcherTableHandle.getColumnsNotFitForDictionary());
        dispatcherTableHandle.getOriginalExpression().ifPresent(builder::originalExpression);
        dispatcherTableHandle.getLimit().ifPresent(builder::limit);
        return builder;
    }

    public Builder builder(int predicateThreshold, ConnectorTableHandle connectorTableHandle)
    {
        SchemaTableName schemaTableName = transformer.getSchemaTableName(connectorTableHandle);
        return new Builder(transformer, predicateThreshold)
                .schemaName(schemaTableName.getSchemaName())
                .tableName(schemaTableName.getTableName())
                .proxiedConnectorTableHandle(connectorTableHandle);
    }

    public static class Builder
    {
        private final DispatcherProxiedConnectorTransformer transformer;
        private String schemaName;
        private String tableName;
        protected final int predicateThreshold;
        protected ConnectorTableHandle proxiedConnectorTableHandle;

        protected OptionalLong limit = OptionalLong.empty();
        protected TupleDomain<ColumnHandle> fullPredicate = TupleDomain.all();
        protected Optional<WarpExpression> warpExpression = Optional.empty();
        protected boolean subsumedPredicates;
        private Metrics metrics = Metrics.EMPTY;
        private Set<String> columnsNotFitForDictionary = Set.of();
        private Optional<ExpressionAndAssignments> originalExpression = Optional.of(ExpressionAndAssignments.TRUE);

        private Builder(DispatcherProxiedConnectorTransformer transformer, int predicateThreshold)
        {
            this.transformer = requireNonNull(transformer);
            this.predicateThreshold = predicateThreshold;
        }

        public Builder schemaName(String schemaName)
        {
            this.schemaName = schemaName;
            return this;
        }

        public Builder tableName(String tableName)
        {
            this.tableName = tableName;
            return this;
        }

        public Builder proxiedConnectorTableHandle(ConnectorTableHandle proxiedConnectorTableHandle)
        {
            this.proxiedConnectorTableHandle = proxiedConnectorTableHandle;
            return this;
        }

        public Builder limit(long limit)
        {
            this.limit = OptionalLong.of(limit);
            return this;
        }

        public Builder fullPredicate(TupleDomain<ColumnHandle> fullPredicate)
        {
            this.fullPredicate = fullPredicate;
            return this;
        }

        public Builder warpExpression(Optional<WarpExpression> warpExpression)
        {
            this.warpExpression = warpExpression;
            return this;
        }

        public Builder metrics(Metrics metrics)
        {
            this.metrics = metrics;
            return this;
        }

        public Builder subsumedPredicates(boolean subsumedPredicates)
        {
            this.subsumedPredicates = subsumedPredicates;
            return this;
        }

        public Builder columnsNotFitForDictionary(Set<String> columnsNotFitForDictionary)
        {
            this.columnsNotFitForDictionary = columnsNotFitForDictionary;
            return this;
        }

        public Builder originalExpression(ExpressionAndAssignments originalExpression)
        {
            this.originalExpression = Optional.of(originalExpression);
            return this;
        }

        public Builder originalExpression(ConnectorExpression originalExpression, Map<String, ColumnHandle> originalAssignments)
        {
            this.originalExpression = Optional.of(new ExpressionAndAssignments(originalExpression, originalAssignments));
            return this;
        }

        public DispatcherTableHandle build()
        {
            return new DispatcherTableHandle(
                    schemaName,
                    tableName,
                    limit,
                    fullPredicate,
                    transformer.getSimplifiedColumns(proxiedConnectorTableHandle, fullPredicate, predicateThreshold),
                    proxiedConnectorTableHandle,
                    warpExpression,
                    metrics,
                    subsumedPredicates,
                    columnsNotFitForDictionary,
                    originalExpression);
        }
    }
}
