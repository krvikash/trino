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
package io.trino.connector;

import com.google.inject.Inject;
import io.trino.execution.TaskManagerConfig;
import io.trino.spi.connector.FileSystemReadExecutor;
import jakarta.annotation.PreDestroy;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.google.common.util.concurrent.MoreExecutors.shutdownAndAwaitTermination;
import static io.airlift.concurrent.Threads.virtualThreadsNamed;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public final class BoundedFileSystemReadExecutor
        implements FileSystemReadExecutor
{
    private final ExecutorService executor;

    @Inject
    public BoundedFileSystemReadExecutor(TaskManagerConfig config)
    {
        this(requireNonNull(config, "config is null").getMaxConcurrentFilesystemReads());
    }

    public BoundedFileSystemReadExecutor(int maxConcurrentReads)
    {
        this.executor = Executors.newFixedThreadPool(
                maxConcurrentReads,
                virtualThreadsNamed("filesystem-read-%s"));
    }

    @Override
    public <T> CompletableFuture<T> submit(Callable<T> task)
    {
        requireNonNull(task, "task is null");
        CompletableFuture<T> future = new CompletableFuture<>();
        executor.execute(() -> {
            try {
                future.complete(task.call());
            }
            catch (Throwable t) {
                future.completeExceptionally(t);
            }
        });
        return future;
    }

    @PreDestroy
    public void shutdown()
    {
        shutdownAndAwaitTermination(executor, 10, SECONDS);
    }
}
