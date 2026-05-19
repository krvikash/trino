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

import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.trino.plugin.warp.annotation.ForWarp;
import io.trino.plugin.warp.cloudvendors.config.CloudVendorConfig;
import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.config.NativeConfig;
import io.trino.plugin.warp.config.WarmupDemoterConfig;
import io.trino.plugin.warp.di.WarpInitializedServiceRegistry;
import io.trino.plugin.warp.dispatcher.WarpMDCContext;
import io.trino.plugin.warp.dispatcher.model.RowGroupKey;
import io.trino.plugin.warp.dispatcher.warmup.export.WeGroupCloudExporterTask;
import io.trino.plugin.warp.gen.stats.WorkerTaskExecutorServiceStats;
import io.trino.plugin.warp.log.ShapingLogger;
import io.trino.plugin.warp.log.ShapingLoggerFactory;
import io.trino.plugin.warp.metrics.MetricsManager;
import io.trino.plugin.warp.storage.engine.nativeimpl.NativeStorageStateHandler;
import io.trino.plugin.warp.util.WarpInitializedServiceMarker;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.ConnectorSession;
import jakarta.annotation.PreDestroy;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.plugin.warp.dispatcher.warmup.WarmUtils.isImportExportEnabled;
import static java.util.Objects.requireNonNull;

@Singleton
public class WorkerTaskExecutorService
        implements WarpInitializedServiceMarker
{
    private static final Logger logger = Logger.get(WorkerTaskExecutorService.class);
    private final ShapingLogger shapingLogger;

    private final NativeConfig nativeConfig;
    private final WorkerTaskExecutorServiceStats statsWorkerTaskExecutorService;
    private final Map<RowGroupKey, UUID> submittedRowGroups = new ConcurrentHashMap<>();
    private final SetMultimap<RowGroupKey, WorkerSubmittableTask> pendingTasks = Multimaps.newSetMultimap(new ConcurrentHashMap<>(), () -> {
        Comparator<WorkerSubmittableTask> c = Comparator.comparing(o -> (o.getPriority() + "." + o.getId()));
        return new TreeSet<>(c.reversed());
    });
    private final ReentrantLock lock = new ReentrantLock();
    private ExecutorService prioritizeExecutorService;
    private ExecutorService cloudExecutorService;
    private ExecutorService proxyExecutorService;
    private ScheduledExecutorService scheduledCloudExecutorService;
    private final NativeStorageStateHandler nativeStorageStateHandler;
    private final int queueSize;
    private final GlobalConfig globalConfig;
    private final CatalogName catalogName;
    private final CloudVendorConfig cloudVendorConfig;
    private boolean isImportExportInitialized;

    @Inject
    public WorkerTaskExecutorService(
            WarmupDemoterConfig warmupDemoterConfig,
            NativeConfig nativeConfig,
            MetricsManager metricsManager,
            GlobalConfig globalConfig,
            @ForWarp CloudVendorConfig cloudVendorConfig,
            NativeStorageStateHandler nativeStorageStateHandler,
            WarpInitializedServiceRegistry warpInitializedServiceRegistry,
            ShapingLoggerFactory shapingLoggerFactory,
            CatalogName catalogName)
    {
        this.nativeConfig = requireNonNull(nativeConfig);
        this.statsWorkerTaskExecutorService = metricsManager.registerMetric(new WorkerTaskExecutorServiceStats());
        this.globalConfig = requireNonNull(globalConfig);
        this.catalogName = catalogName;
        requireNonNull(warmupDemoterConfig);
        this.queueSize = warmupDemoterConfig.getTasksExecutorQueueSize();
        this.cloudVendorConfig = requireNonNull(cloudVendorConfig);
        this.nativeStorageStateHandler = requireNonNull(nativeStorageStateHandler);
        warpInitializedServiceRegistry.addService(this);
        shapingLogger = shapingLoggerFactory.getInstance(this.getClass());
    }

    @Override
    public void init()
    {
        prioritizeExecutorService = getPrioritizeExecutorService();
        initImportExport(null);
        proxyExecutorService = getProxyExecutorService(nativeConfig.getTaskMaxWorkerThreads());
    }

    public void initImportExport(ConnectorSession session)
    {
        if (!isImportExportInitialized && isImportExportEnabled(globalConfig, cloudVendorConfig, session)) {
            cloudExecutorService = getCloudExecutorService();
            scheduledCloudExecutorService = getScheduledCloudExecutorService();
            isImportExportInitialized = true;
        }
    }

    @PreDestroy
    public void shutdown()
    {
        lock.lock();
        try {
            prioritizeExecutorService.shutdown();
            if (isImportExportInitialized) {
                cloudExecutorService.shutdown();
                scheduledCloudExecutorService.shutdown();
            }
            proxyExecutorService.shutdown();

            prioritizeExecutorService.awaitTermination(10, TimeUnit.SECONDS);
            if (isImportExportInitialized) {
                cloudExecutorService.awaitTermination(10, TimeUnit.SECONDS);
                scheduledCloudExecutorService.awaitTermination(10, TimeUnit.SECONDS);
            }
            proxyExecutorService.awaitTermination(10, TimeUnit.SECONDS);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        finally {
            lock.unlock();
        }
    }

    private ExecutorService getPrioritizeExecutorService()
    {
        int poolSize = getPoolSize(globalConfig.getPrioritizeExecutorPoolSize());
        return new ThreadPoolExecutor(
                poolSize,
                poolSize,
                60L,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(queueSize),
                new ThreadFactoryBuilder().setNameFormat("warp-speed-prioritize-%s").setDaemon(true).build());
    }

    private ExecutorService getCloudExecutorService()
    {
        int poolSize = globalConfig.getCloudExecutorPoolSize();
        BlockingQueue<Runnable> blockingQueue = new PriorityBlockingQueue<>(
                queueSize,
                Comparator.comparingDouble(x -> ((WorkerSubmittableTask) x).getPriority()).reversed());
        return new ThreadPoolExecutor(
                poolSize,
                poolSize,
                60L,
                TimeUnit.SECONDS,
                blockingQueue,
                new ThreadFactoryBuilder().setNameFormat("warp-speed-cloud-%s").setDaemon(true).build());
    }

    private ScheduledExecutorService getScheduledCloudExecutorService()
    {
        int poolSize = globalConfig.getCloudExecutorPoolSize();
        return new ScheduledThreadPoolExecutor(poolSize, daemonThreadsNamed("warp-speed-worker-task-executor-%s"));
    }

    private ExecutorService getProxyExecutorService(int numWorkerThreads)
    {
        int poolSize = getPoolSize(numWorkerThreads);
        BlockingQueue<Runnable> blockingQueue = new PriorityBlockingQueue<>(
                queueSize,
                Comparator.comparingDouble(x -> ((WorkerSubmittableTask) x).getPriority()).reversed());

        return new ThreadPoolExecutor(
                poolSize,
                poolSize,
                60L,
                TimeUnit.SECONDS,
                blockingQueue,
                new ThreadFactoryBuilder().setNameFormat("warp-speed-proxy-%s").setDaemon(true).build());
    }

    private int getPoolSize(int size)
    {
        return globalConfig.isDebugWarmingSingleThreaded() ? 1 : size;
    }

    public SubmissionResult submitTask(WorkerSubmittableTask task, boolean allowConflicts)
    {
        if (!nativeStorageStateHandler.isStorageAvailable()) {
            return SubmissionResult.REJECTED;
        }

        SubmissionResult ret = SubmissionResult.SCHEDULED;
        lock.lock();
        try {
            if (submittedRowGroups.size() + pendingTasks.size() < queueSize) {
                UUID newTaskId = task.getId();
                try {
                    UUID savedTaskId = submittedRowGroups.computeIfAbsent(task.getRowGroupKey(), _ -> {
                        statsWorkerTaskExecutorService.inctask_scheduled();
                        task.taskScheduled();
                        executeTask(task);
                        return task.getId();
                    });
                    if (!savedTaskId.equals(newTaskId)) {
                        if (allowConflicts) {
                            statsWorkerTaskExecutorService.inctask_pended();
                            if (pendingTasks.get(task.getRowGroupKey()).size() < 3) { // prevent too many conflicts on the same key
                                ret = SubmissionResult.CONFLICT;
                                pendingTasks.put(task.getRowGroupKey(), task);
                            }
                            else {
                                statsWorkerTaskExecutorService.inctask_skipped_due_queue_size();
                                ret = SubmissionResult.REJECTED;
                            }
                        }
                        else {
                            ret = SubmissionResult.REJECTED;
                        }
                    }
                }
                catch (RejectedExecutionException e) {
                    shapingLogger.warn("too many elements in the warming queue dropping key: %s", task.getRowGroupKey());
                    statsWorkerTaskExecutorService.inctask_skipped_due_queue_size();
                    ret = SubmissionResult.REJECTED;
                }
            }
            else {
                statsWorkerTaskExecutorService.inctask_skipped_due_queue_size();
                ret = SubmissionResult.REJECTED;
            }
        }
        finally {
            lock.unlock();
        }
        return ret;
    }

    private void executeTask(WorkerSubmittableTask task)
    {
        try (WarpMDCContext _ = new WarpMDCContext(catalogName.toString(), Optional.of(task.getClass().getSimpleName()))) {
            if (task instanceof PrioritizeTask) {
                prioritizeExecutorService.execute(task);
            }
            else if (task instanceof ImportExecutionTask || task instanceof WeGroupCloudExporterTask) {
                cloudExecutorService.execute(task);
            }
            else if (task instanceof ProxyExecutionTask) {
                proxyExecutorService.execute(task);
            }
            else if (task instanceof WarpCacheTask) {
                proxyExecutorService.execute(task);
            }
        }
    }

    public void taskFinished(RowGroupKey rowGroupKey)
    {
        lock.lock();
        try {
            if (nativeStorageStateHandler.isStorageAvailable()) {
                submittedRowGroups.remove(rowGroupKey);
                Set<WorkerSubmittableTask> pendingRowGroupTasks = pendingTasks.get(rowGroupKey);
                if (!pendingRowGroupTasks.isEmpty()) {
                    Iterator<WorkerSubmittableTask> iterator = pendingRowGroupTasks.iterator();
                    WorkerSubmittableTask workerSubmittableTask = iterator.next();
                    iterator.remove();
                    statsWorkerTaskExecutorService.inctask_resubmitted();
                    submitTask(workerSubmittableTask, true);
                }
                statsWorkerTaskExecutorService.inctask_finished();
            }
        }
        finally {
            lock.unlock();
        }
    }

    public void delaySubmit(long delayInSeconds, WorkerSubmittableTask task, Consumer<WorkerSubmittableTask> conflictCallback)
    {
        if (nativeStorageStateHandler.isStorageAvailable()) {
            if (delayInSeconds > 0) {
                statsWorkerTaskExecutorService.inctask_delayed();
                DelayedTask delayedTask = new DelayedTask(this, task, conflictCallback);
                ScheduledFuture<?> _ = scheduledCloudExecutorService.schedule(delayedTask, delayInSeconds, TimeUnit.SECONDS);
            }
            else {
                SubmissionResult submissionResult = submitTask(task, true);
                if ((submissionResult == SubmissionResult.CONFLICT) && (conflictCallback != null)) {
                    try {
                        conflictCallback.accept(task);
                    }
                    catch (Exception e) {
                        shapingLogger.warn("failed to call conflict callback for task %s", task.getRowGroupKey());
                    }
                }
            }
        }
    }

    public enum SubmissionResult
    {
        SCHEDULED,
        REJECTED,
        CONFLICT,
    }

    public enum TaskExecutionType
    {
        CLASSIFY(0),
        PROXY(0),
        // import end export tasks run in the same executor, priority will impact which task will be handled first.
        // import should be handled before export
        IMPORT(10),
        EXPORT(0),
        CACHE(0);

        private final int priority;

        TaskExecutionType(int priority)
        {
            this.priority = priority;
        }

        public int getPriority()
        {
            return priority;
        }
    }

    private record DelayedTask(
            @SuppressWarnings("unused") WorkerTaskExecutorService workerTaskExecutorService,
            @SuppressWarnings("unused") WorkerSubmittableTask task,
            @SuppressWarnings("unused") Consumer<WorkerSubmittableTask> conflictCallback)
            implements Runnable
    {
        @Override
        public void run()
        {
            SubmissionResult submissionResult = workerTaskExecutorService().submitTask(task(), true);
            if ((submissionResult == SubmissionResult.CONFLICT) && (conflictCallback != null)) {
                try {
                    conflictCallback.accept(task());
                }
                catch (Exception e) {
                    logger.warn("failed to call conflict callback for task %s", task().getRowGroupKey());
                }
            }
        }
    }
}
