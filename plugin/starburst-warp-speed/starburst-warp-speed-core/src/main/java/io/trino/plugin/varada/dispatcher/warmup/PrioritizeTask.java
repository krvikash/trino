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
package io.trino.plugin.varada.dispatcher.warmup;

import io.trino.plugin.varada.config.GlobalConfig;
import io.trino.plugin.varada.dispatcher.DispatcherSplit;
import io.trino.plugin.varada.dispatcher.DispatcherTableHandle;
import io.trino.plugin.varada.dispatcher.model.RowGroupKey;
import io.trino.plugin.varada.dispatcher.query.classifier.QueryClassifier;
import io.trino.plugin.varada.dispatcher.services.RowGroupDataService;
import io.trino.plugin.varada.dispatcher.warmup.warmers.WarmingManager;
import io.trino.plugin.varada.dispatcher.warmup.warmers.WarmupElementsCreator;
import io.trino.plugin.warp.gen.stats.WarmingServiceStats;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.SchemaTableName;
import io.varada.cloudvendors.config.CloudVendorConfig;

import java.util.List;
import java.util.Optional;

import static io.trino.plugin.varada.dispatcher.warmup.WarmUtils.isImportExportEnabled;

public class PrioritizeTask
        extends WorkerWarmerBaseTask
{
    private final GlobalConfig globalConfig;
    private final CloudVendorConfig cloudVendorConfig;

    public PrioritizeTask(WarmExecutionTaskFactory warmExecutionTaskFactory,
            WorkerWarmingService workerWarmingService,
            ConnectorPageSourceProvider connectorPageSourceProvider,
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            DispatcherTableHandle dispatcherTableHandle,
            RowGroupKey rowGroupKey,
            List<ColumnHandle> columns,
            DispatcherSplit dispatcherSplit,
            DynamicFilter dynamicFilter,
            RowGroupDataService rowGroupDataService,
            QueryClassifier queryClassifier,
            WorkerTaskExecutorService workerTaskExecutorService,
            int iterationCount,
            WarmingServiceStats statsWarmingService,
            WarmingManager warmingManager, WarmupElementsCreator warmupElementsCreator,
            GlobalConfig globalConfig,
            CloudVendorConfig cloudVendorConfig)
    {
        super(warmExecutionTaskFactory, workerTaskExecutorService, statsWarmingService, warmingManager, workerWarmingService, connectorPageSourceProvider, transactionHandle, session, dispatcherTableHandle, rowGroupKey, columns, dispatcherSplit, dynamicFilter, rowGroupDataService, queryClassifier, warmupElementsCreator, iterationCount);
        this.globalConfig = globalConfig;
        this.cloudVendorConfig = cloudVendorConfig;
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
            warmingManager.warmEmptyRowGroup(rowGroupKey, warmupElementsCreator.createWarmupElements(rowGroupKey,
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
        return warmExecutionTaskFactory.createExecutionTask(connectorPageSourceProvider,
                transactionHandle,
                session,
                dispatcherSplit,
                dispatcherTableHandle,
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
