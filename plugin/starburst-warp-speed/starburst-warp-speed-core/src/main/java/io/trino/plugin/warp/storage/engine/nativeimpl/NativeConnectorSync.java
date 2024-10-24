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
package io.trino.plugin.warp.storage.engine.nativeimpl;

import com.google.common.eventbus.EventBus;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.config.NativeConfig;
import io.trino.plugin.warp.dispatcher.warmup.demoter.WarmupDemoterService;
import io.trino.plugin.warp.gen.constants.DemoteStatus;
import io.trino.plugin.warp.storage.capacity.WorkerCapacityManager;
import io.trino.plugin.warp.storage.engine.ConnectorSync;
import io.trino.plugin.warp.storage.engine.ConnectorSyncInitializedEvent;
import io.trino.plugin.warp.storage.engine.QueryMemory;
import io.trino.plugin.warp.storage.read.StorageCollectorCallBack;
import io.trino.spi.catalog.CatalogName;
import jakarta.annotation.PreDestroy;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SegmentAllocator;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.Objects.requireNonNull;

@Singleton
public class NativeConnectorSync
        implements ConnectorSync
{
    private static final Logger logger = Logger.get(NativeConnectorSync.class);
    private static final int ALLOC_ALIGNMENT = Integer.BYTES;

    private final CatalogName catalogName;
    private final EventBus eventBus;
    private final ExecutorService executorService = Executors.newSingleThreadExecutor(daemonThreadsNamed("warp-speed-native-connector-sync-%s"));
    private final WorkerCapacityManager workerCapacityManager;
    private final GlobalConfig globalConfig;
    private final NativeConfig nativeConfig;
    private WarmupDemoterService warmupDemoterService;
    private MemorySegment catalogContext;
    private MemorySegment sharedConnectorMemory;
    private MemorySegment[] queryMemories;

    // syncher API
    private final MethodHandle mGetContextSize;
    private final MethodHandle mSetSharedConnectorMemory;
    private final MethodHandle mUnregister;
    private final MethodHandle mAllocQueryMemoryId;
    private final MethodHandle mFreeQueryMemoryId;
    private final MethodHandle mDemotePrepare;
    private final MethodHandle mDemoteStart;
    private final MethodHandle mDemoteEnd;

    @Inject
    public NativeConnectorSync(CatalogName catalogName,
                               EventBus eventBus,
                               GlobalConfig globalConfig,
                               NativeConfig nativeConfig,
                               WorkerCapacityManager workerCapacityManager)
    {
        try {
            SymbolLookup libraryHandle = SymbolLookup.loaderLookup();
            Linker linker = Linker.nativeLinker();

            mGetContextSize = linker.downcallHandle(libraryHandle.find("syncher_get_context_size").orElseThrow(),
                    FunctionDescriptor.of(ValueLayout.JAVA_INT));
            mSetSharedConnectorMemory = linker.downcallHandle(libraryHandle.find("syncher_set_shared_connector_memory").orElseThrow(),
                    FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS));
            mUnregister = linker.downcallHandle(libraryHandle.find("syncher_unregister").orElseThrow(),
                    FunctionDescriptor.of(ValueLayout.JAVA_BOOLEAN, ValueLayout.ADDRESS));
            mAllocQueryMemoryId = linker.downcallHandle(libraryHandle.find("syncher_alloc_reader_id").orElseThrow(),
                    FunctionDescriptor.of(ValueLayout.JAVA_INT));
            mFreeQueryMemoryId = linker.downcallHandle(libraryHandle.find("syncher_free_reader_id").orElseThrow(),
                    FunctionDescriptor.ofVoid(ValueLayout.JAVA_INT));
            mDemotePrepare = linker.downcallHandle(libraryHandle.find("syncher_demote_prepare").orElseThrow(),
                    FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.JAVA_DOUBLE));
            mDemoteStart = linker.downcallHandle(libraryHandle.find("syncher_demote_start").orElseThrow(),
                    FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_INT));
            mDemoteEnd = linker.downcallHandle(libraryHandle.find("syncher_demote_end").orElseThrow(),
                    FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_INT, ValueLayout.JAVA_DOUBLE, ValueLayout.JAVA_DOUBLE, ValueLayout.JAVA_INT));

            this.catalogName = catalogName;
            this.eventBus = requireNonNull(eventBus);
            this.workerCapacityManager = requireNonNull(workerCapacityManager);
            this.globalConfig = requireNonNull(globalConfig);
            this.nativeConfig = requireNonNull(nativeConfig);

            int contextSize = (int) mGetContextSize.invokeExact();
            if (contextSize <= 0) {
                throw new RuntimeException("failed to get native connector context size");
            }
            this.catalogContext = Arena.ofAuto().allocate(contextSize + ALLOC_ALIGNMENT, ALLOC_ALIGNMENT);
        }
        catch (Throwable t) {
            logger.error(t, "failed loading native connector");
            throw new RuntimeException(t);
        }
    }

    @Override
    public void init(WarmupDemoterService warmupDemoterService)
    {
        this.warmupDemoterService = warmupDemoterService;

        try {
            final int numWorkerThreads = nativeConfig.getTaskMaxWorkerThreads();
            checkArgument(numWorkerThreads > 0, "no segments configured for match bitmaps");
            final long memorySizePerWorker = (long) globalConfig.getMatchMemorySize() + (long) globalConfig.getCollectMemorySize();
            final long sharedConnectorMemorySize = memorySizePerWorker * (long) numWorkerThreads;
            // register and get memory address. note that the name is not passed to native. no need.
            long sharedConnectorMemoryAddress = register(catalogContext.address(), StorageCollectorCallBack.class);
            // in case no memory was allocated yet, allocate it
            if (sharedConnectorMemoryAddress == 0) {
                // allocate the memory as global so it will leave even if this connector is unregistered
                sharedConnectorMemory = Arena.global().allocate(sharedConnectorMemorySize + ALLOC_ALIGNMENT, ALLOC_ALIGNMENT);
                // set the address and check if it was the one taken
                sharedConnectorMemoryAddress = (long) mSetSharedConnectorMemory.invokeExact(sharedConnectorMemory);
                if (sharedConnectorMemoryAddress != sharedConnectorMemory.address()) {
                    sharedConnectorMemory = null; // throwing it away since another one was kept in storage engine
                }
            }

            // create the memory segment if needed
            if (sharedConnectorMemory == null) {
                // can happen if we never allocated it or threw it away
                sharedConnectorMemory = MemorySegment.ofAddress(sharedConnectorMemoryAddress).reinterpret(sharedConnectorMemorySize);
            }

            // create the native resources queue
            SegmentAllocator nativeAllocator = SegmentAllocator.slicingAllocator(sharedConnectorMemory);
            queryMemories = new MemorySegment[numWorkerThreads];
            for (int id = 0; id < queryMemories.length; id++) {
                queryMemories[id] = nativeAllocator.allocate(memorySizePerWorker, ALLOC_ALIGNMENT);
            }

            // complete the regisgtration
            logger.info("catalog name %s sharedConnectorMemory %s", catalogName, sharedConnectorMemory);
            eventBus.post(new ConnectorSyncInitializedEvent(true));
        }
        catch (Throwable t) {
            logger.error(t, "failed to register");
            throw new RuntimeException(t);
        }
    }

    @PreDestroy
    public void shutdown()
    {
        try {
            boolean success = (boolean) mUnregister.invokeExact(catalogContext);
            if (!success) {
                catalogContext = null;
                sharedConnectorMemory = null;
                logger.error("syncer failed to unregister");
                return;
            }
            workerCapacityManager.deleteLocalStorageFiles();
            catalogContext = null;
            sharedConnectorMemory = null;
            logger.info("unregister catalog name %s", catalogName);
        }
        catch (Throwable t) {
            logger.error(t, "failed to unregister");
        }
    }

    @Override
    public String getCatalogName()
    {
        return catalogName.toString();
    }

    @Override
    public boolean isDefaultCatalog()
    {
        return (readCatalogId() == 0);
    }

    @Override
    public boolean isCatalogReducedResources()
    {
        return (readCatalogId() >= 4);
    }

    @Override
    public long getCatalogContext()
    {
        return catalogContext.address();
    }

    private int readCatalogId()
    {
        return catalogContext.get(ValueLayout.JAVA_INT, 0);
    }

    // native resources API
    @Override
    public QueryMemory allocQueryMemory()
    {
        try {
            int queryMemoryId = (int) mAllocQueryMemoryId.invokeExact();
            if ((queryMemoryId >= 0) && (queryMemoryId < queryMemories.length)) {
                return new QueryMemory(queryMemoryId, queryMemories[queryMemoryId]);
            }
        }
        catch (Throwable t) {
            logger.error(t, "failed to allocate query memory");
        }
        throw new RuntimeException("failed to allocate query memory");
    }

    @Override
    public void freeQueryMemory(int queryMemoryId)
    {
        try {
            mFreeQueryMemoryId.invokeExact(queryMemoryId);
        }
        catch (Throwable t) {
            logger.error(t, "failed to free query memory");
            throw new RuntimeException("failed to free query memory");
        }
    }

    // demote API
    @Override
    public int syncDemotePrepare(double epsilon)
    {
        try {
            logger.debug("%s -call syncDemotePrepare with epsilon=%f", catalogName, epsilon);
            return (int) mDemotePrepare.invokeExact(catalogContext, epsilon);
        }
        catch (Throwable t) {
            logger.error(t, "failed to demote prepare");
            throw new RuntimeException("failed to demote prepare");
        }
    }

    @Override
    public void startDemote(int demoteSequence)
    {
        try {
            logger.debug("%s - call syncDemoteStart with demoteSequence =%d", catalogName, demoteSequence);
            mDemoteStart.invokeExact(catalogContext, demoteSequence);
        }
        catch (Throwable t) {
            logger.error(t, "failed to demote start");
            throw new RuntimeException("failed to demote start");
        }
    }

    @Override
    public void syncDemoteEnd(int demoteSequence, double lowestPriorityExist, double highestPriorityDemoted, DemoteStatus demoteStatus)
    {
        try {
            logger.debug("%s -call syncDemoteCycleEnd with demoteSequence=%d, lowestPriorityExist=%f, highestPriorityDemoted=%f, demoteStatus=%s, demoteStatusOrdinal=%d",
                    catalogName, demoteSequence, lowestPriorityExist, highestPriorityDemoted, demoteStatus.name(), demoteStatus.ordinal());
            mDemoteEnd.invokeExact(catalogContext, demoteSequence, lowestPriorityExist, highestPriorityDemoted, demoteStatus.ordinal());
        }
        catch (Throwable t) {
            logger.error(t, "failed to demote end");
            throw new RuntimeException("failed to demote end");
        }
    }

    // demote callbacks
    public void callback_GetLowestPriority(int demoteSequence)
    {
        logger.debug("%s - callback_GetLowestPriority, demoteSequence = %d",
                catalogName, demoteSequence);
        if (warmupDemoterService == null) {
            logger.error("warmupDemoterCatalogService == null");
            return;
        }
        Future<?> unused = executorService.submit(() -> warmupDemoterService.connectorSyncStartDemote(demoteSequence));
    }

    public void callback_DemoteStart(int demoteSequence, double maxPriorityToDemote, boolean isSingleConnector)
    {
        logger.debug("%s - callback_DemoteStart, demoteSequence=%d, maxPriorityToDemote=%f, isSingleConnector=%b", catalogName, demoteSequence, maxPriorityToDemote, isSingleConnector);
        if (warmupDemoterService == null) {
            logger.error("warmupDemoterCatalogService == null");
            return;
        }
        Future<?> unused = executorService.submit(() -> warmupDemoterService.connectorSyncStartDemoteCycle(maxPriorityToDemote, isSingleConnector));
    }

    public void callback_DemoteEnd(int demoteSequence, double highestPriority)
    {
        logger.debug("%s - callback_demoteEnd, demoteSequence=%d, highestPriority=%f", catalogName, demoteSequence, highestPriority);
        if (warmupDemoterService == null) {
            logger.error("%s - warmupDemoterCatalogService == null", catalogName);
            return;
        }
        Future<?> unused = executorService.submit(() -> warmupDemoterService.connectorSyncDemoteEnd(demoteSequence, highestPriority));
    }

    private native long register(long context, Class<StorageCollectorCallBack> storageCollector);
}
