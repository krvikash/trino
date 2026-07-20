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
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.DynamicFilterSnapshot;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.trino.testing.InterfaceTestUtils.assertAllMethodsOverridden;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DispatcherSplitSourceTest
{
    final int maxSize = 100;
    private List<ConnectorSplit> connectorSplits;
    private DispatcherSplitSource dispatcherSplitSource;

    @BeforeEach
    public void before()
    {
        ConnectorSplitSource connectorSplitSource = mock(ConnectorSplitSource.class);

        connectorSplits = IntStream.range(0, 10)
                .mapToObj(i -> new DispatcherSplit(
                        "database",
                        "table",
                        "path" + i,
                        0,
                        1,
                        2L,
                        List.of(),
                        "",
                        mock(ConnectorSplit.class)))
                .collect(Collectors.toList());
        CompletableFuture<List<ConnectorSplit>> connectorSplitBatchCompletableFuture = CompletableFuture.completedFuture(connectorSplits);
        when(connectorSplitSource.getNextBatch(eq(maxSize), eq(DynamicFilterSnapshot.EMPTY))).thenReturn(connectorSplitBatchCompletableFuture);

        dispatcherSplitSource = new DispatcherSplitSource(
                connectorSplitSource,
                mock(DispatcherTableHandle.class),
                mock(ConnectorSession.class),
                new TestingConnectorProxiedConnectorTransformer());
    }

    @Test
    public void testGetNextBatch()
            throws ExecutionException, InterruptedException
    {
        CompletableFuture<List<ConnectorSplit>> splitBatchCompletableFuture = dispatcherSplitSource.getNextBatch(maxSize, DynamicFilterSnapshot.EMPTY);

        List<ConnectorSplit> connectorSplitsResult1 = splitBatchCompletableFuture.get();

        assertThat(connectorSplits.size()).isEqualTo(connectorSplitsResult1.size());

        for (int i = 0; i < connectorSplits.size(); i++) {
            DispatcherSplit hiveSplit = (DispatcherSplit) connectorSplits.get(i);
            DispatcherSplit connectorSplitResult = (DispatcherSplit) connectorSplitsResult1.get(i);
            assertThat(hiveSplit.getPath()).isEqualTo(connectorSplitResult.getPath());
            assertThat(connectorSplitResult.getAffinityKey()).contains(hiveSplit.getPath() + ":" + hiveSplit.getStart() + ":" + hiveSplit.getLength());
        }
    }

    @Test
    public void testEverythingImplemented()
    {
        assertAllMethodsOverridden(
                ConnectorSplitSource.class,
                DispatcherSplitSource.class);
    }
}
