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
package io.trino.plugin.warp.storage.memory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.log.ShapingLogger;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;

@Singleton
public class WorkerMemoryManager
{
    private static final Logger logger = Logger.get(WorkerMemoryManager.class);

    private final ShapingLogger shapingLogger;
    private final AtomicLong numOffHeapBytes;
    private final AtomicLong numOffHeapGcBytes;
    private final ExecutorService executorService;

    @Inject
    public WorkerMemoryManager(GlobalConfig globalConfig)
    {
        this.numOffHeapBytes = new AtomicLong();
        this.numOffHeapGcBytes = new AtomicLong();
        this.executorService = Executors.newFixedThreadPool(1, daemonThreadsNamed("warp-speed-memory-manager-%s"));
        this.shapingLogger = ShapingLogger.getInstance(
                logger,
                globalConfig.getShapingLoggerThreshold(),
                globalConfig.getShapingLoggerDuration(),
                globalConfig.getShapingLoggerNumberOfSamples());
    }

    public ThreadArena getThreadArena()
    {
        return new ThreadArena(this::onClose, numOffHeapBytes, shapingLogger);
    }

    public GcArena getGcArena()
    {
        return new GcArena(this::limitReached, numOffHeapGcBytes, shapingLogger);
    }

    // defining ad Void(Void) to allow passing this as a callback function
    Void onClose(Void v)
    {
        if ((numOffHeapBytes.get() > ThreadArena.MAX_ALLOCATED_BYTES) || (numOffHeapGcBytes.get() > GcArena.MAX_ALLOCATED_BYTES)) {
            shapingLogger.warn("reached off heap limit: numOffHeapBytes %d numOffHeapGcBytes %d", numOffHeapBytes.get(), numOffHeapGcBytes.get());
        }
        return null;
    }

    // defining ad Void(Void) to allow passing this as a callback function
    Void limitReached(Void v)
    {
        executorService.execute(() -> {
            long bytesToFreeInGc = numOffHeapGcBytes.get();
            if (bytesToFreeInGc > GcArena.MAX_ALLOCATED_BYTES) {
                shapingLogger.warn("reached limit of numOffHeapGcBytes %d", bytesToFreeInGc);
                System.gc();
                numOffHeapGcBytes.addAndGet(-1 * bytesToFreeInGc);
            }
        });
        return null;
    }
}
