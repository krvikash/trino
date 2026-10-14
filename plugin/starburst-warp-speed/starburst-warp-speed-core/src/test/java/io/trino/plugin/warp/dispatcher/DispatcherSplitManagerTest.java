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

import io.trino.plugin.warp.connector.TestingConnectorProxiedConnectorTransformer;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.testing.TestingTransactionHandle;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static io.trino.spi.connector.FixedSplitSource.emptySplitSource;
import static io.trino.testing.InterfaceTestUtils.assertAllMethodsOverridden;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;

public class DispatcherSplitManagerTest
{
    @Test
    public void testEverythingImplemented()
            throws NoSuchMethodException
    {
        assertAllMethodsOverridden(
                ConnectorSplitManager.class,
                DispatcherSplitManager.class,
                Set.of(
                        // Warp does not expose cache metadata, so its splits have no cache id
                        ConnectorSplitManager.class.getMethod("getCacheSplitId", ConnectorSplit.class)));
    }

    @Test
    public void testTableFunctionSplitsComeFromProxiedConnector()
    {
        RecordingSplitManager proxiedSplitManager = new RecordingSplitManager();
        DispatcherSplitManager splitManager = new DispatcherSplitManager(proxiedSplitManager, new TestingConnectorProxiedConnectorTransformer());
        ConnectorTransactionHandle transaction = TestingTransactionHandle.create();
        ConnectorTableFunctionHandle function = new ConnectorTableFunctionHandle() {};

        ConnectorSplitSource splitSource = splitManager.getSplits(transaction, SESSION, function);

        assertThat(splitSource).isSameAs(proxiedSplitManager.splitSource);
        assertThat(proxiedSplitManager.lastTransaction).isSameAs(transaction);
        assertThat(proxiedSplitManager.lastFunction).isSameAs(function);
    }

    private static final class RecordingSplitManager
            implements ConnectorSplitManager
    {
        private final ConnectorSplitSource splitSource = emptySplitSource();
        private ConnectorTransactionHandle lastTransaction;
        private ConnectorTableFunctionHandle lastFunction;

        @Override
        public ConnectorSplitSource getSplits(
                ConnectorTransactionHandle transaction,
                ConnectorSession session,
                ConnectorTableHandle table,
                Set<ColumnHandle> dynamicFilterColumns,
                Constraint constraint)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorTableFunctionHandle function)
        {
            lastTransaction = transaction;
            lastFunction = function;
            return splitSource;
        }
    }
}
