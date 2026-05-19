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
import io.trino.plugin.warp.log.ShapingLogger;
import io.trino.plugin.warp.log.ShapingLoggerFactory;
import io.trino.spi.catalog.CatalogName;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;

@Singleton
public class WorkerMemoryManager
{
    private final ShapingLogger shapingLogger;
    private final AtomicLong numOffHeapBytes;
    private final AtomicLong numOffHeapGcBytes;
    private final AtomicLong numOffHeapPinnedGcBytes;
    private final ExecutorService executorService;
    private final CatalogName catalogName;

    @Inject
    public WorkerMemoryManager(CatalogName catalogName, ShapingLoggerFactory shapingLoggerFactory)
    {
        this.numOffHeapBytes = new AtomicLong();
        this.numOffHeapGcBytes = new AtomicLong();
        this.numOffHeapPinnedGcBytes = new AtomicLong();
        this.executorService = Executors.newFixedThreadPool(1, daemonThreadsNamed("warp-speed-memory-manager-%s"));
        this.catalogName = catalogName;
        this.shapingLogger = shapingLoggerFactory.getInstance(WorkerMemoryManager.class);
    }

    public ThreadArena getThreadArena()
    {
        return new ThreadArena(this::onClose, numOffHeapBytes, shapingLogger);
    }

    public GcArena getGcArena()
    {
        return new GcArena(this::limitReached, numOffHeapGcBytes, shapingLogger);
    }

    public PinnedGcArena getPinnedGcArena()
    {
        return new PinnedGcArena(numOffHeapPinnedGcBytes, shapingLogger);
    }

    void onClose()
    {
        long logGcLimit = GcArena.MAX_ALLOCATED_BYTES * 2;
        if ((numOffHeapBytes.get() > ThreadArena.MAX_ALLOCATED_BYTES) || (numOffHeapGcBytes.get() + numOffHeapPinnedGcBytes.get() > logGcLimit)) {
            shapingLogger.warn(
                    "catalog %s reached off heap limit: numOffHeapBytes %d numOffHeapGcBytes %d numOffHeapPinnedGcBytes %d",
                    catalogName,
                    numOffHeapBytes.get(),
                    numOffHeapGcBytes.get(),
                    numOffHeapPinnedGcBytes.get());
        }
    }

    void limitReached()
    {
        executorService.execute(() -> {
            long bytesToFreeInGc = numOffHeapGcBytes.get();
            if (bytesToFreeInGc > GcArena.MAX_ALLOCATED_BYTES) {
                System.gc();
                long numOffHeapGcBytesAfterGc = numOffHeapGcBytes.addAndGet(-1 * bytesToFreeInGc);
                if (numOffHeapGcBytesAfterGc > GcArena.MAX_ALLOCATED_BYTES) {
                    shapingLogger.warn(
                            "catalog %s still on limit: bytesToFreeInGc %d numOffHeapGcBytesAfterGc %d",
                            catalogName,
                            bytesToFreeInGc,
                            numOffHeapGcBytesAfterGc);
                }
            }
        });
    }
}
