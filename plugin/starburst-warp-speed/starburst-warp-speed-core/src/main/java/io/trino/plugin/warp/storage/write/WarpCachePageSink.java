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

package io.trino.plugin.warp.storage.write;

import io.airlift.slice.Slice;
import io.trino.plugin.warp.dispatcher.warmup.WarpCacheTask;
import io.trino.plugin.warp.dispatcher.warmup.WorkerTaskExecutorService;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorPageSink;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

public class WarpCachePageSink
        implements ConnectorPageSink
{
    private final WarpCacheTask warpCacheTask;
    private final WorkerTaskExecutorService workerTaskExecutorService;
    private long memoryUsage;
    private boolean firstTime;
    private boolean abort;

    public WarpCachePageSink(WarpCacheTask warpCacheTask, WorkerTaskExecutorService workerTaskExecutorService)
    {
        this.workerTaskExecutorService = workerTaskExecutorService;
        this.warpCacheTask = warpCacheTask;
        this.firstTime = true;
        this.abort = false;
    }

    @Override
    public long getMemoryUsage()
    {
        return memoryUsage;
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        if (abort) {
            return NOT_BLOCKED;
        }
        memoryUsage = warpCacheTask.addPage(page);
        if (firstTime) {
            WorkerTaskExecutorService.SubmissionResult submissionResult = workerTaskExecutorService.submitTask(warpCacheTask, false);
            if (submissionResult != WorkerTaskExecutorService.SubmissionResult.SCHEDULED) {
                warpCacheTask.clean();
                abort = true;
            }
            firstTime = false;
        }
        return NOT_BLOCKED;
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        if (abort) {
            return CompletableFuture.completedFuture(Collections.emptyList());
        }
        if (firstTime) {
            warpCacheTask.warmAsEmptyPageSource();
        }
        else {
            warpCacheTask.setFinished();
        }
        return CompletableFuture.completedFuture(Collections.emptyList());
    }

    @Override
    public void abort()
    {
        if (abort) {
            return;
        }
        warpCacheTask.setEngineAbort();
    }
}
