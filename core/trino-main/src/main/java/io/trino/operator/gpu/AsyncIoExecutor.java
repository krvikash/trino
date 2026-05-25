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
package io.trino.operator.gpu;

import com.google.inject.Inject;
import io.trino.execution.TaskManagerConfig;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.gpu.IoExecutor;
import jakarta.annotation.PreDestroy;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;

import static com.google.common.util.concurrent.MoreExecutors.shutdownAndAwaitTermination;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public final class AsyncIoExecutor
        implements IoExecutor
{
    private final ExecutorService executor;

    @Inject
    public AsyncIoExecutor(GpuConfig gpuConfig, TaskManagerConfig taskManagerConfig)
    {
        this(maxConcurrentReads(gpuConfig, taskManagerConfig));
    }

    public AsyncIoExecutor(int maxConcurrentReads)
    {
        this.executor = Executors.newFixedThreadPool(
                maxConcurrentReads,
                daemonThreadsNamed("filesystem-read-%s"));
    }

    private static int maxConcurrentReads(GpuConfig gpuConfig, TaskManagerConfig taskManagerConfig)
    {
        requireNonNull(gpuConfig, "gpuConfig is null");
        requireNonNull(taskManagerConfig, "taskManagerConfig is null");
        return gpuConfig.getMaxConcurrentReads()
                .orElseGet(() -> {
                    // A worker runs up to maxWorkerThreads splits concurrently and each split may issue
                    // several filesystem reads at once, so allow headroom above the worker-thread count, capped at 64.
                    return min(taskManagerConfig.getMaxWorkerThreads() * 8, 64);
                });
    }

    @Override
    public <T> CompletableFuture<T> submit(Callable<T> task)
    {
        requireNonNull(task, "task is null");
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        CompletableFuture<T> future = new CompletableFuture<>();
        try {
            executor.execute(() -> {
                try (ThreadContextClassLoader _ = new ThreadContextClassLoader(contextClassLoader)) {
                    future.complete(task.call());
                }
                catch (Throwable t) {
                    future.completeExceptionally(t);
                }
            });
        }
        catch (RejectedExecutionException e) {
            future.completeExceptionally(e);
        }
        return future;
    }

    @PreDestroy
    public void shutdown()
    {
        shutdownAndAwaitTermination(executor, 10, SECONDS);
    }
}
