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
package io.trino.plugin.base;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.subquery.cache.CacheSplitId;
import jakarta.inject.Inject;

import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class DecoratingConnectorSplitManager
        implements ConnectorSplitManager
{
    private final ConnectorSplitManager delegate;

    @Inject
    public DecoratingConnectorSplitManager(@ForDecorator ConnectorSplitManager splitManager, Set<Decorator<ConnectorSplitManager>> decorators)
    {
        this.delegate = Decorator.combine(
                () -> requireNonNull(splitManager, "splitManager is null"),
                requireNonNull(decorators, "decorators is null"));
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            Set<ColumnHandle> dynamicFilterColumns,
            Constraint constraint)
    {
        return delegate.getSplits(transaction, session, table, dynamicFilterColumns, constraint);
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableFunctionHandle function)
    {
        return delegate.getSplits(transaction, session, function);
    }

    @Override
    public Optional<CacheSplitId> getCacheSplitId(ConnectorSplit split)
    {
        return delegate.getCacheSplitId(split);
    }
}
