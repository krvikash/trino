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
package io.trino.plugin.warp.dispatcher.warmup.warmers;

import io.trino.plugin.warp.TestingTxService;
import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.config.SharedConfig;
import io.trino.plugin.warp.dispatcher.services.RowGroupDataService;
import io.trino.plugin.warp.dispatcher.warmup.demoter.WarmupDemoterService;
import io.trino.plugin.warp.juffer.StorageEngineTxService;
import io.trino.plugin.warp.log.ShapingLoggerFactory;
import io.trino.plugin.warp.storage.capacity.WorkerCapacityManager;
import io.trino.plugin.warp.storage.engine.StubsStorageEngine;
import io.trino.plugin.warp.storage.engine.nativeimpl.NativeStorageStateHandler;
import io.trino.plugin.warp.storage.flows.FlowsSequencer;
import io.trino.spi.catalog.CatalogName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class StorageWarmerServiceTest
{
    @Test
    public void testTryToAcquireWarmupLoader()
    {
        WarmupDemoterService warmupDemoterService = mock(WarmupDemoterService.class);
        when(warmupDemoterService.tryAllocateNativeResourceForWarmup()).thenReturn(true, false, false);
        StorageWarmerService storageWarmerService = new StorageWarmerService(
                mock(RowGroupDataService.class),
                new StubsStorageEngine(),
                new GlobalConfig(),
                warmupDemoterService,
                mock(StorageEngineTxService.class),
                mock(FlowsSequencer.class),
                TestingTxService.createMetricsManager(),
                mock(WorkerCapacityManager.class),
                mock(NativeStorageStateHandler.class),
                new ShapingLoggerFactory(new CatalogName("c"), new SharedConfig()));
        assertThat(storageWarmerService.tryAllocateNativeResourceForWarmup()).isTrue();
        assertThat(storageWarmerService.tryAllocateNativeResourceForWarmup()).isFalse();
        assertThat(storageWarmerService.tryAllocateNativeResourceForWarmup()).isFalse();
    }
}
