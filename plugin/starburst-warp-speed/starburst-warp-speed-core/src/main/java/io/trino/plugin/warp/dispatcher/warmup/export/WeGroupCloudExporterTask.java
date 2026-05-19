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
package io.trino.plugin.warp.dispatcher.warmup.export;

import io.airlift.log.Logger;
import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.dispatcher.model.FastWarmingState;
import io.trino.plugin.warp.dispatcher.model.RowGroupData;
import io.trino.plugin.warp.dispatcher.model.RowGroupKey;
import io.trino.plugin.warp.dispatcher.services.RowGroupDataService;
import io.trino.plugin.warp.dispatcher.warmup.WorkerSubmittableTask;
import io.trino.plugin.warp.dispatcher.warmup.WorkerTaskExecutorService;
import io.trino.plugin.warp.gen.stats.WarmupExportServiceStats;
import io.trino.plugin.warp.log.ShapingLogger;
import io.trino.plugin.warp.log.ShapingLoggerFactory;
import io.trino.plugin.warp.tools.util.StopWatch;

import java.util.UUID;

import static java.util.Objects.requireNonNull;

public class WeGroupCloudExporterTask
        implements WorkerSubmittableTask
{
    private static final Logger logger = Logger.get(WeGroupCloudExporterTask.class);

    private final UUID id;
    private final RowGroupKey rowGroupKey;
    private final String cloudImportExportPath;
    private final WorkerTaskExecutorService workerTaskExecutorService;
    private final RowGroupDataService rowGroupDataService;
    private final WarmupElementsCloudExporter warmupElementsCloudExporter;
    private final GlobalConfig globalConfig;
    private final WarmupExportServiceStats statsWarmupExportService;
    private final ShapingLogger shapingLogger;

    public WeGroupCloudExporterTask(
            RowGroupKey rowGroupKey,
            String cloudImportExportPath,
            WorkerTaskExecutorService workerTaskExecutorService,
            RowGroupDataService rowGroupDataService,
            WarmupElementsCloudExporter warmupElementsCloudExporter,
            GlobalConfig globalConfig,
            WarmupExportServiceStats statsWarmupExportService,
            ShapingLoggerFactory shapingLoggerFactory)
    {
        this.shapingLogger = shapingLoggerFactory.getInstance(this.getClass());

        this.rowGroupKey = requireNonNull(rowGroupKey);
        this.cloudImportExportPath = requireNonNull(cloudImportExportPath);
        this.workerTaskExecutorService = requireNonNull(workerTaskExecutorService);
        this.rowGroupDataService = requireNonNull(rowGroupDataService);
        this.warmupElementsCloudExporter = requireNonNull(warmupElementsCloudExporter);
        this.globalConfig = requireNonNull(globalConfig);
        this.statsWarmupExportService = requireNonNull(statsWarmupExportService);
        this.id = UUID.randomUUID();
    }

    @Override
    public UUID getId()
    {
        return id;
    }

    @Override
    public int getPriority()
    {
        return WorkerTaskExecutorService.TaskExecutionType.EXPORT.getPriority();
    }

    @Override
    public RowGroupKey getRowGroupKey()
    {
        return rowGroupKey;
    }

    @Override
    public void taskScheduled()
    {
        statsWarmupExportService.incexport_row_group_scheduled();
    }

    @Override
    public void run()
    {
        boolean resubmit = false;
        try {
            resubmit = !exportWeGroup();
        }
        finally {
            workerTaskExecutorService.taskFinished(rowGroupKey);
            statsWarmupExportService.incexport_row_group_finished();
            if (resubmit) {
                workerTaskExecutorService.delaySubmit(0, this, null);
            }
        }
    }

    // return ture if finished, false if we need to resend
    private boolean exportWeGroup()
    {
        RowGroupData rowGroupData = rowGroupDataService.get(rowGroupKey);
        // if no row group data or no elements to export do nothing
        if ((rowGroupData == null) || rowGroupData.isEmpty()) {
            return true;
        }
        if (shouldNotExportRowGroupData(rowGroupData)) {
            statsWarmupExportService.incexport_skipped_due_key_demoted_row_group();
            return true;
        }

        StopWatch stopWatch = new StopWatch();
        boolean success = false;
        boolean locked = false;
        try {
            // try to acquire read lock since we cannot export the file while it is being updated
            locked = rowGroupData.getLock().readLock();
            if (!locked) {
                logger.warn("rowGroupKey %s cannot export since its locked, will try later", rowGroupKey);
                return false;
            }

            // do the actual work
            stopWatch.start();
            statsWarmupExportService.incexport_row_group_started();

            WarmupElementsCloudExporter.ExportFileResults exportFileResults =
                    warmupElementsCloudExporter.exportFile(rowGroupData, cloudImportExportPath);
            if (exportFileResults.isExportDone()) {
                logger.debug(
                        "exported rowGroupKey %s next-offset %d next-export-offset %d dataValidation %s",
                        rowGroupKey,
                        rowGroupData.getNextOffset(),
                        rowGroupData.getNextExportOffset(),
                        exportFileResults.dataValidation());
                // update row group with new export state
                rowGroupDataService.save(RowGroupData.builder(rowGroupData)
                        .nextExportOffset(rowGroupData.getNextOffset())
                        .fastWarmingState(FastWarmingState.EXPORTED)
                        .dataValidation(exportFileResults.dataValidation())
                        .build());
                success = true;
            }
        }
        catch (Exception e) {
            shapingLogger.error("export failed rowGroupKey %s exception: %s cause: %s", rowGroupKey, e, e.getCause());
            if (stopWatch.isStarted()) {
                // update row group with new export state
                RowGroupData updated = updatedRowGroupWithExportFailedState(rowGroupData);
                rowGroupDataService.save(updated);
            }
        }
        finally {
            if (stopWatch.isStarted()) {
                stopWatch.stop();
                statsWarmupExportService.addexport_time(stopWatch.getNanoTime());
            }
            if (locked) {
                rowGroupData.getLock().readUnLock();
                if (success) {
                    statsWarmupExportService.incexport_row_group_accomplished();
                }
                else {
                    statsWarmupExportService.incexport_row_group_failed();
                }
            }
        }
        return true;
    }

    private boolean shouldNotExportRowGroupData(RowGroupData rowGroupData)
    {
        return !((FastWarmingState.State.NOT_EXPORTED.equals(rowGroupData.getFastWarmingState().state()) ||
                FastWarmingState.State.FAILED_TEMPORARILY.equals(rowGroupData.getFastWarmingState().state())) &&
                (rowGroupData.getNextExportOffset() < rowGroupData.getNextOffset()));
    }

    public RowGroupData updatedRowGroupWithExportFailedState(RowGroupData rowGroupData)
    {
        long lastTemporaryFailure = System.currentTimeMillis();
        return RowGroupData.builder(rowGroupData)
                .fastWarmingState(addTemporaryFailure(rowGroupData.getFastWarmingState(), lastTemporaryFailure))
                .build();
    }

    private FastWarmingState addTemporaryFailure(FastWarmingState fastWarmingState, long lastTemporaryFailure)
    {
        if (FastWarmingState.State.FAILED_PERMANENTLY.equals(fastWarmingState.state()) ||
                fastWarmingState.temporaryFailureCount() >= globalConfig.getMaxWarmRetries()) {
            return FastWarmingState.FAILED_PERMANENTLY;
        }

        int failureCount = fastWarmingState.temporaryFailureCount() + 1;
        return new FastWarmingState(FastWarmingState.State.FAILED_TEMPORARILY, failureCount, lastTemporaryFailure);
    }
}
