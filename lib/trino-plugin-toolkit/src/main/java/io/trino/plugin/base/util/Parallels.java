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
package io.trino.plugin.base.util;

import io.airlift.log.Logger;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static com.google.common.base.Verify.verify;
import static io.airlift.concurrent.MoreFutures.getDone;

public final class Parallels
{
    private Parallels() {}

    private static final Logger log = Logger.get(Parallels.class);

    /**
     * Call {@code process} on each of the {@code inputs} elements, using calling thread and up to {@code additionalBackgroundThreads}
     * scheduled within {@code executor}.
     * <p>
     * Any exceptions raised from {@code process} are eventually propagated to the caller.
     * <p>
     * This method propagates {@link Context#current()} into tasks it starts within the executor.
     */
    public static <T> void processWithAdditionalThreads(
            Executor executor,
            int additionalBackgroundThreads,
            Collection<T> inputs,
            Consumer<T> process)
    {
        LinkedBlockingQueue<T> taskQueue = new LinkedBlockingQueue<>(inputs);
        AtomicInteger runningTasks = new AtomicInteger();
        Context tracingContext = Context.current();
        AtomicBoolean failure = new AtomicBoolean(false);
        Runnable processQueue = () -> {
            try (Scope ignore = tracingContext.makeCurrent()) {
                runningTasks.incrementAndGet();
                while (!failure.get()) {
                    T next = taskQueue.poll();
                    if (next == null) {
                        break;
                    }
                    process.accept(next);
                }
                runningTasks.decrementAndGet();
            }
            catch (Throwable e) {
                failure.set(true);
                throw e;
            }
        };
        // Do not use background threads for singleton input.
        int jobCount = Math.min(additionalBackgroundThreads, taskQueue.size() - 1);
        ExecutorCompletionService<Void> completionService = new ExecutorCompletionService<>(executor);
        List<Future<?>> allFutures = new ArrayList<>();
        try {
            for (int job = jobCount; job > 0; job--) {
                try {
                    allFutures.add(completionService.submit(processQueue, null));
                }
                catch (RejectedExecutionException e) {
                    // When this happens it may affect multiple callers. Do not call full stacktrace to avoid log explosion.
                    log.warn("processWithAdditionalThreads: background processing thread pool is full: %s", e);
                    break;
                }
            }
            // Use main thread to ensure the use of background processing threads increases parallelism.
            // Otherwise, for larger number of callers the shared pool could be a bottleneck.
            processQueue.run();

            // At this point the taskQueue is empty, and we only need to wait for completion of "last few" tasks already in flight.
            // The in-flight tasks are counted by runningTasks. Ignore and orphan those that didn't have a chance to start
            // due to thread pool being full.
            while (runningTasks.get() > 0) {
                try {
                    // Wait and check for success
                    getDone(completionService.take());
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted", e);
                }
            }

            verify(!failure.get(), "There was a failure, but exception was not propagated");
        }
        finally {
            allFutures.forEach(future -> future.cancel(true));
        }
    }
}
