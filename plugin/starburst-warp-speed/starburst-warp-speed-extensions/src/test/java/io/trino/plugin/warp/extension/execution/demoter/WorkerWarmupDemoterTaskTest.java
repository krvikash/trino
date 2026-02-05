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
package io.trino.plugin.warp.extension.execution.demoter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.eventbus.EventBus;
import io.airlift.json.ObjectMapperProvider;
import io.trino.plugin.warp.config.WarmupDemoterConfig;
import io.trino.plugin.warp.dispatcher.warmup.demoter.WarmupDemoterService;
import io.trino.plugin.warp.dispatcher.warmup.demoter.WarpDeleteService;
import io.trino.plugin.warp.dispatcher.warmup.demoter.events.WarmupDemoterFinishEvent;
import io.trino.plugin.warp.extension.execution.debugtools.WarmupDemoterData;
import io.trino.plugin.warp.extension.execution.debugtools.WarmupDemoterThreshold;
import io.trino.plugin.warp.extension.execution.debugtools.WorkerWarmupDemoterTask;
import io.trino.plugin.warp.gen.stats.WarmupDemoterStats;
import io.trino.plugin.warp.metrics.MetricsManager;
import io.trino.plugin.warp.storage.capacity.WorkerCapacityManager;
import io.trino.plugin.warp.storage.engine.nativeimpl.NativeStorageStateHandler;
import io.trino.spi.catalog.CatalogName;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

public class WorkerWarmupDemoterTaskTest
{
    final ExecutorService executorService = Executors.newSingleThreadExecutor();
    private WorkerWarmupDemoterTask workerWarmupDemoterTask;
    private WarmupDemoterService warmupDemoterService;
    private WorkerCapacityManager workerCapacityManager;
    private WarmupDemoterConfig warmupDemoterConfig;
    private EventBus eventBus;
    private NativeStorageStateHandler nativeStorageStateHandler;

    @BeforeEach
    public void before()
    {
        warmupDemoterConfig = new WarmupDemoterConfig();
        workerCapacityManager = mock(WorkerCapacityManager.class);
        warmupDemoterService = mock(WarmupDemoterService.class);
        WarpDeleteService warpDeleteService = mock(WarpDeleteService.class);
        eventBus = new EventBus();
        nativeStorageStateHandler = mock(NativeStorageStateHandler.class);
        Mockito.when(nativeStorageStateHandler.isStorageAvailable()).thenReturn(true);
        WarmupDemoterStats warmupDemoterStats = WarmupDemoterStats.create();
        MetricsManager metricsManager = mock(MetricsManager.class);
        Mockito.when(metricsManager.registerMetric(ArgumentMatchers.any())).thenReturn(warmupDemoterStats);
        workerWarmupDemoterTask = new WorkerWarmupDemoterTask(
                warmupDemoterService,
                warpDeleteService,
                warmupDemoterConfig,
                workerCapacityManager,
                mock(CatalogName.class),
                metricsManager,
                eventBus,
                nativeStorageStateHandler);
    }

    @Test
    public void testCalculatedThreshold()
    {
        Mockito.when(workerCapacityManager.getCurrentUsage()).thenReturn(2048L);
        Mockito.when(workerCapacityManager.getTotalCapacity()).thenReturn(4096L);
        Mockito.when(warmupDemoterService.tryDemoteStart()).thenAnswer(_ -> {
            Future<?> _ = executorService.submit(() -> eventBus.post(new WarmupDemoterFinishEvent(true, new HashMap<>())));
            return 1;
        });
        WarmupDemoterData warmupDemoterData = WarmupDemoterData.builder()
                .batchSize(1)
                .executeDemoter(true)
                .modifyConfig(true)
                .warmupDemoterThreshold(new WarmupDemoterThreshold(0.9, 0.7))
                .build();
        workerWarmupDemoterTask.start(warmupDemoterData);
        Assertions.assertThat(warmupDemoterConfig.getMaxUsageThresholdPercentage()).isEqualTo(45d);
        Assertions.assertThat(warmupDemoterConfig.getCleanupUsageThresholdPercentage()).isEqualTo(35d);
        Assertions.assertThat(warmupDemoterConfig.getBatchSize()).isEqualTo(1);
    }

    @Test
    public void testDefaultValues()
            throws JsonProcessingException
    {
        ObjectMapper objectMapper = new ObjectMapperProvider().get();
        String jsonStr = "{\"@class\":\"io.trino.plugin.warp.extension.execution.debugtools.WarmupDemoterData\"}";
        WarmupDemoterData warmupDemoterData = objectMapper.readerFor(WarmupDemoterData.class).readValue(jsonStr);
        assertThat(warmupDemoterData.getBatchSize()).isEqualTo(-1);
    }

    @Test
    public void testStorageNotAvailable()
    {
        Mockito.when(nativeStorageStateHandler.isStorageAvailable()).thenReturn(false);
        Map<String, Object> actualResult = workerWarmupDemoterTask.start(WarmupDemoterData.builder().build());
        assertThat(actualResult).containsAllEntriesOf(workerWarmupDemoterTask.getSkippedResult());
    }
}
