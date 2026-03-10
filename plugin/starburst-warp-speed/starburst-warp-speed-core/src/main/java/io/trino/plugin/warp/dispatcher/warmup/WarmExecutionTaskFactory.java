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
package io.trino.plugin.warp.dispatcher.warmup;

import com.google.common.eventbus.EventBus;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.trino.plugin.warp.annotation.ForWarp;
import io.trino.plugin.warp.cloudvendors.config.CloudVendorConfig;
import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.dispatcher.DispatcherProxiedConnectorTransformer;
import io.trino.plugin.warp.dispatcher.DispatcherSplit;
import io.trino.plugin.warp.dispatcher.DispatcherTableHandle;
import io.trino.plugin.warp.dispatcher.model.RowGroupKey;
import io.trino.plugin.warp.dispatcher.query.classifier.QueryClassifier;
import io.trino.plugin.warp.dispatcher.services.RowGroupDataService;
import io.trino.plugin.warp.dispatcher.warmup.warmers.StorageWarmerService;
import io.trino.plugin.warp.dispatcher.warmup.warmers.WarmingManager;
import io.trino.plugin.warp.dispatcher.warmup.warmers.WarmupElementsCreator;
import io.trino.plugin.warp.gen.stats.WarmingServiceStats;
import io.trino.plugin.warp.log.ShapingLoggerFactory;
import io.trino.plugin.warp.metrics.MetricsManager;
import io.trino.plugin.warp.storage.engine.nativeimpl.NativeStorageStateHandler;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.TableCredentials;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

@Singleton
public class WarmExecutionTaskFactory
{
    private final DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer;
    private final EventBus eventBus;
    private final WarmingManager warmingManager;
    private final WarmingServiceStats statsWarmingService;
    private final QueryClassifier queryClassifier;
    private final RowGroupDataService rowGroupDataService;
    private final GlobalConfig globalConfig;
    private final WarmupElementsCreator warmupElementsCreator;
    private final WorkerTaskExecutorService workerTaskExecutorService;
    private final StorageWarmerService storageWarmerService;
    private final NativeStorageStateHandler nativeStorageStateHandler;
    private final CloudVendorConfig cloudVendorConfig;
    private final ShapingLoggerFactory shapingLoggerFactory;

    @Inject
    public WarmExecutionTaskFactory(DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer,
            EventBus eventBus,
            WarmingManager warmingManager,
            MetricsManager metricsManager,
            QueryClassifier queryClassifier,
            RowGroupDataService rowGroupDataService,
            GlobalConfig globalConfig,
            WarmupElementsCreator warmupElementsCreator,
            WorkerTaskExecutorService workerTaskExecutorService,
            StorageWarmerService storageWarmerService,
            NativeStorageStateHandler nativeStorageStateHandler,
            @ForWarp CloudVendorConfig cloudVendorConfig,
            ShapingLoggerFactory shapingLoggerFactory)
    {
        this.dispatcherProxiedConnectorTransformer = requireNonNull(dispatcherProxiedConnectorTransformer);
        this.eventBus = requireNonNull(eventBus);
        this.warmingManager = requireNonNull(warmingManager);
        this.queryClassifier = requireNonNull(queryClassifier);
        this.rowGroupDataService = requireNonNull(rowGroupDataService);
        this.statsWarmingService = metricsManager.registerMetric(WarmingServiceStats.create());
        this.globalConfig = requireNonNull(globalConfig);
        this.warmupElementsCreator = requireNonNull(warmupElementsCreator);
        this.workerTaskExecutorService = requireNonNull(workerTaskExecutorService);
        this.storageWarmerService = requireNonNull(storageWarmerService);
        this.nativeStorageStateHandler = requireNonNull(nativeStorageStateHandler);
        this.cloudVendorConfig = requireNonNull(cloudVendorConfig);
        this.shapingLoggerFactory = requireNonNull(shapingLoggerFactory);
    }

    public WorkerSubmittableTask createExecutionTask(ConnectorPageSourceProvider connectorPageSourceProvider,
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            DispatcherSplit dispatcherSplit,
            DispatcherTableHandle dispatcherTableHandle,
            Optional<TableCredentials> tableCredentials,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter,
            RowGroupKey rowGroupKey,
            WorkerWarmingService workerWarmingService,
            int iterationCount,
            int executionTaskPriority,
            WorkerTaskExecutorService.TaskExecutionType taskExecutionType)
    {
        return switch (taskExecutionType) {
            case CLASSIFY -> new PrioritizeTask(this,
                    workerWarmingService,
                    connectorPageSourceProvider,
                    transactionHandle,
                    session,
                    dispatcherTableHandle,
                    tableCredentials,
                    rowGroupKey,
                    columns,
                    dispatcherSplit,
                    dynamicFilter,
                    rowGroupDataService,
                    queryClassifier,
                    workerTaskExecutorService,
                    iterationCount,
                    statsWarmingService,
                    warmingManager,
                    warmupElementsCreator,
                    nativeStorageStateHandler,
                    globalConfig,
                    cloudVendorConfig,
                    shapingLoggerFactory);
            case PROXY -> new ProxyExecutionTask(this,
                    eventBus,
                    dispatcherProxiedConnectorTransformer,
                    warmingManager,
                    statsWarmingService,
                    workerWarmingService,
                    connectorPageSourceProvider,
                    transactionHandle,
                    session,
                    dispatcherTableHandle,
                    tableCredentials,
                    rowGroupKey,
                    columns,
                    dispatcherSplit,
                    dynamicFilter,
                    rowGroupDataService,
                    globalConfig,
                    queryClassifier,
                    warmupElementsCreator,
                    nativeStorageStateHandler,
                    iterationCount,
                    executionTaskPriority,
                    workerTaskExecutorService,
                    storageWarmerService,
                    shapingLoggerFactory);
            case IMPORT -> new ImportExecutionTask(this,
                    statsWarmingService,
                    workerWarmingService,
                    shapingLoggerFactory,
                    connectorPageSourceProvider,
                    transactionHandle,
                    session,
                    dispatcherTableHandle,
                    tableCredentials,
                    rowGroupKey,
                    columns,
                    dispatcherSplit,
                    dynamicFilter,
                    rowGroupDataService,
                    queryClassifier,
                    workerTaskExecutorService,
                    warmingManager,
                    warmupElementsCreator,
                    nativeStorageStateHandler,
                    iterationCount,
                    executionTaskPriority);
            default -> throw new RuntimeException("no task exists");
        };
    }
}
