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

import io.trino.plugin.warp.dispatcher.DispatcherSplit;
import io.trino.plugin.warp.dispatcher.DispatcherTableHandle;
import io.trino.plugin.warp.dispatcher.model.RowGroupData;
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

import java.util.List;
import java.util.Optional;

public class ImportExecutionTask
        extends WorkerWarmerBaseTask
{
    private final int executionTaskPriority;

    public ImportExecutionTask(
            WarmExecutionTaskFactory warmExecutionTaskFactory,
            WarmingServiceStats statsWarmingService,
            WorkerWarmingService workerWarmingService,
            ShapingLoggerFactory shapingLoggerFactory,
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
            WarmingManager warmingManager,
            WarmupElementsCreator warmupElementsCreator,
            NativeStorageStateHandler nativeStorageStateHandler,
            int iterationCount,
            int executionTaskPriority)
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
        this.executionTaskPriority = executionTaskPriority;
    }

    @Override
    public int getPriority()
    {
        return WorkerTaskExecutorService.TaskExecutionType.IMPORT.getPriority() + executionTaskPriority;
    }

    @Override
    public void taskScheduled()
    {
        statsWarmingService.incwarm_scheduled();
    }

    @Override
    protected void warm(WarmData dataToWarm)
    {
        Optional<RowGroupData> importedRowGroupData = warmingManager.importWeGroup(session, rowGroupKey, dataToWarm.warmWarmUpElements());
        boolean finishToWarm = false;
        if (importedRowGroupData.isPresent()) {
            RowGroupData rowGroupData = importedRowGroupData.get();
            dataToWarm = workerWarmingService.updateWarmData(rowGroupData, dataToWarm);
            finishToWarm = dataToWarm.warmExecutionState() != WarmExecutionState.WARM;
        }
        if (!finishToWarm) {
            nextTask = Optional.of(createProxyExecutionTask((int) dataToWarm.highestPriority()));
        }
    }

    @Override
    protected WarmData getWarmData()
    {
        return getWarmData(true);
    }
}
