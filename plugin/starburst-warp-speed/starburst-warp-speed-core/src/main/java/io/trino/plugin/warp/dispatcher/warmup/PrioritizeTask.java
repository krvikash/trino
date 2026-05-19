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

import io.trino.plugin.warp.cloudvendors.config.CloudVendorConfig;
import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.dispatcher.DispatcherSplit;
import io.trino.plugin.warp.dispatcher.DispatcherTableHandle;
import io.trino.plugin.warp.dispatcher.model.RowGroupKey;
import io.trino.plugin.warp.dispatcher.query.classifier.QueryClassifier;
import io.trino.plugin.warp.dispatcher.services.RowGroupDataService;
import io.trino.plugin.warp.dispatcher.warmup.warmers.WarmingManager;
import io.trino.plugin.warp.dispatcher.warmup.warmers.WarmupElementsCreator;
import io.trino.plugin.warp.gen.stats.WarmingServiceStats;
import io.trino.plugin.warp.log.ShapingLoggerFactory;
import io.trino.plugin.warp.storage.engine.nativeimpl.NativeStorageStateHandler;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.SchemaTableName;

import java.util.List;
import java.util.Optional;

import static io.trino.plugin.warp.dispatcher.warmup.WarmUtils.isImportExportEnabled;
import static java.util.Objects.requireNonNull;

public class PrioritizeTask
        extends WorkerWarmerBaseTask
{
    private final GlobalConfig globalConfig;
    private final CloudVendorConfig cloudVendorConfig;

    public PrioritizeTask(
            WarmExecutionTaskFactory warmExecutionTaskFactory,
            WorkerWarmingService workerWarmingService,
            ConnectorPageSourceProvider connectorPageSourceProvider,
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            DispatcherTableHandle dispatcherTableHandle,
            Optional<ConnectorTableCredentials> tableCredentials,
            RowGroupKey rowGroupKey,
            List<ColumnHandle> columns,
            DispatcherSplit dispatcherSplit,
            DynamicFilter dynamicFilter,
            RowGroupDataService rowGroupDataService,
            QueryClassifier queryClassifier,
            WorkerTaskExecutorService workerTaskExecutorService,
            int iterationCount,
            WarmingServiceStats statsWarmingService,
            WarmingManager warmingManager,
            WarmupElementsCreator warmupElementsCreator,
            NativeStorageStateHandler nativeStorageStateHandler,
            GlobalConfig globalConfig,
            CloudVendorConfig cloudVendorConfig,
            ShapingLoggerFactory shapingLoggerFactory)
    {
        super(warmExecutionTaskFactory,
                workerTaskExecutorService,
                statsWarmingService,
                warmingManager,
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
                warmupElementsCreator,
                nativeStorageStateHandler,
                iterationCount);
        this.globalConfig = requireNonNull(globalConfig);
        this.cloudVendorConfig = requireNonNull(cloudVendorConfig);
    }

    @Override
    public int getPriority()
    {
        return 0;
    }

    @Override
    public void taskScheduled()
    {
        statsWarmingService.incwarm_scheduled();
    }

    @Override
    protected void warm(WarmData dataToWarm)
    {
        if (dataToWarm.warmExecutionState().equals(WarmExecutionState.EMPTY_ROW_GROUP)) {
            warmingManager.warmEmptyRowGroup(rowGroupKey, warmupElementsCreator.createWarmupElements(
                    rowGroupKey,
                    dataToWarm.requiredWarmUpTypeMap(),
                    new SchemaTableName(rowGroupKey.schema(), rowGroupKey.table()),
                    dataToWarm.columnHandleList()));
            return;
        }
        if (isImportExportEnabled(globalConfig, cloudVendorConfig, session)) {
            nextTask = Optional.of(createImportTask((int) dataToWarm.highestPriority()));
        }
        else {
            nextTask = Optional.of(createProxyExecutionTask((int) dataToWarm.highestPriority()));
        }
    }

    private WorkerSubmittableTask createImportTask(int priority)
    {
        return warmExecutionTaskFactory.createExecutionTask(
                connectorPageSourceProvider,
                transactionHandle,
                session,
                dispatcherSplit,
                dispatcherTableHandle,
                tableCredentials,
                columns,
                dynamicFilter,
                rowGroupKey,
                workerWarmingService,
                iterationCount,
                priority,
                WorkerTaskExecutorService.TaskExecutionType.IMPORT);
    }

    @Override
    protected WarmData getWarmData()
    {
        return getWarmData(true);
    }
}
