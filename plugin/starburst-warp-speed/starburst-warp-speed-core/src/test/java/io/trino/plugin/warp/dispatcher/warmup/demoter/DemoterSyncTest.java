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
package io.trino.plugin.warp.dispatcher.warmup.demoter;

import io.trino.plugin.warp.log.ShapingLoggerFactory;
import io.trino.plugin.warp.storage.flows.FlowsSequencer;
import io.trino.spi.Node;
import io.trino.spi.NodeManager;
import io.trino.spi.catalog.CatalogName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DemoterSyncTest
{
    private DemoterSync demoterSync;
    private FlowsSequencer flowsSequencer;
    private NodeManager nodeManager;

    @BeforeEach
    public void before()
    {
        demoterSync = new DemoterSync(mock(ShapingLoggerFactory.class));
        flowsSequencer = new FlowsSequencer();
        nodeManager = mockNodeManager();
    }

    @Test
    public void testUnregisterWhileDemotingOneCatalog()
            throws InterruptedException
    {
        CatalogName catalogName = new CatalogName("c1");
        WarmupDemoterService warmupDemoterService = mock(WarmupDemoterService.class);
        long demoteKey = demoterSync.registerCatalog(warmupDemoterService, flowsSequencer, catalogName, nodeManager);

        AtomicBoolean isStarted = new AtomicBoolean(false);
        AtomicBoolean isFinished = new AtomicBoolean(false);
        doAnswer(_ -> {
            isStarted.set(true);
            Thread.sleep(500);  // unregisterCatalog() should wait for connectorSyncStartDemote() to finish
            isFinished.set(true);
            return null;
        }).when(warmupDemoterService).connectorSyncStartDemote(anyDouble(), anyDouble(), anyInt(), anyLong(), anyDouble(), anyBoolean(), anyBoolean(), anyBoolean());

        Thread thread = new Thread(() -> demoterSync.tryStartDemoteProcess(demoteKey, 0.5, 0.2, 10, 1000, 0.1, true, false, true));
        thread.start();
        Thread.sleep(200);

        assertThat(isStarted.get()).isTrue();
        assertThat(isFinished).isFalse();
        demoterSync.unregisterCatalog(demoteKey);
        assertThat(isFinished).isTrue();
    }

    @Test
    public void testUnregisterWhileDemotingTwoCatalogs()
            throws InterruptedException
    {
        CatalogName catalogName1 = new CatalogName("c1");
        WarmupDemoterService warmupDemoterService1 = mock(WarmupDemoterService.class);
        long demoteKey1 = demoterSync.registerCatalog(warmupDemoterService1, flowsSequencer, catalogName1, nodeManager);

        CatalogName catalogName2 = new CatalogName("c2");
        WarmupDemoterService warmupDemoterService2 = mock(WarmupDemoterService.class);
        demoterSync.registerCatalog(warmupDemoterService2, flowsSequencer, catalogName2, nodeManager);

        AtomicBoolean isStarted1 = new AtomicBoolean(false);
        AtomicBoolean isFinished1 = new AtomicBoolean(false);
        doAnswer(_ -> {
            isStarted1.set(true);
            Thread.sleep(500);  // unregisterCatalog() should wait for connectorSyncStartDemote() to finish
            isFinished1.set(true);
            return null;
        }).when(warmupDemoterService1).connectorSyncStartDemote(anyDouble(), anyDouble(), anyInt(), anyLong(), anyDouble(), anyBoolean(), anyBoolean(), anyBoolean());

        AtomicBoolean isFinished2 = new AtomicBoolean(false);
        doAnswer(_ -> {
            Thread.sleep(2_000); // unregisterCatalog() should not wait for this to finish as it was called on a different catalog
            isFinished2.set(true);
            return null;
        }).when(warmupDemoterService2).connectorSyncStartDemote(anyDouble(), anyDouble(), anyInt(), anyLong(), anyDouble(), anyBoolean(), anyBoolean(), anyBoolean());

        Thread thread = new Thread(() -> demoterSync.tryStartDemoteProcess(demoteKey1, 0.5, 0.2, 10, 1000, 0.1, true, false, true));
        thread.start();
        Thread.sleep(200);

        assertThat(isStarted1.get()).isTrue();
        assertThat(isFinished1).isFalse();
        assertThat(isFinished2).isFalse();
        demoterSync.unregisterCatalog(demoteKey1);
        assertThat(isFinished1).isTrue();
        assertThat(isFinished2).isFalse();
    }

    private static NodeManager mockNodeManager()
    {
        NodeManager nodeManager = mock(NodeManager.class);
        Node node = mock(Node.class);
        when(node.getNodeIdentifier()).thenReturn("nodeIdentifier1");
        when(nodeManager.getCurrentNode()).thenReturn(node);
        return nodeManager;
    }
}
