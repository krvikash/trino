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
import io.trino.plugin.warp.storage.engine.ConnectorSync;
import io.trino.plugin.warp.storage.engine.ConnectorSyncInitializedEvent;
import io.trino.spi.catalog.CatalogName;
import jakarta.annotation.PreDestroy;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@Singleton
public class NativeConnectorSync
        implements ConnectorSync
{
    private static final Logger logger = Logger.get(NativeConnectorSync.class);
    private static final int ALLOC_ALIGNMENT = Integer.BYTES;

    private final CatalogName catalogName;
    private final EventBus eventBus;
    private final GlobalConfig globalConfig;
    private final NativeConfig nativeConfig;
    private WarmupDemoterService warmupDemoterService;

    private int numWorkerThreads;
    private MemorySegment catalogContext;

    // syncher API
    private final MethodHandle mGetContextSize;
    private final MethodHandle mUnregister;
    private final MethodHandle mAllocReaderId;
    private final MethodHandle mFreeReaderId;

    @Inject
    public NativeConnectorSync(
            CatalogName catalogName,
            EventBus eventBus,
            GlobalConfig globalConfig,
            NativeConfig nativeConfig)
    {
        try {
            SymbolLookup libraryHandle = SymbolLookup.loaderLookup();
            Linker linker = Linker.nativeLinker();

            // syncher API
            mGetContextSize = linker.downcallHandle(libraryHandle.find("syncher_get_context_size").orElseThrow(),
                    FunctionDescriptor.of(ValueLayout.JAVA_INT));
            mUnregister = linker.downcallHandle(libraryHandle.find("syncher_unregister").orElseThrow(),
                    FunctionDescriptor.of(ValueLayout.JAVA_BOOLEAN, ValueLayout.ADDRESS));
            mAllocReaderId = linker.downcallHandle(libraryHandle.find("syncher_alloc_reader_id").orElseThrow(),
                    FunctionDescriptor.of(ValueLayout.JAVA_INT));
            mFreeReaderId = linker.downcallHandle(libraryHandle.find("syncher_free_reader_id").orElseThrow(),
                    FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_INT));

            this.catalogName = catalogName;
            this.eventBus = requireNonNull(eventBus);
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

    public void init()
    {
        try {
            this.numWorkerThreads = nativeConfig.getTaskMaxWorkerThreads();
            checkArgument(numWorkerThreads > 0, "no segments configured for match bitmaps");
            // register and get memory address. note that the name is not passed to native. no need.
            if (register(catalogContext.address()) < 0) {
                throw new RuntimeException("catalog failed to register on too many catalogs");
            }
            // complete the regisgtration
            logger.info("catalog name %s registered", catalogName);
            eventBus.post(new ConnectorSyncInitializedEvent(true));
        }
        catch (Throwable t) {
            logger.error(t, "failed to register");
            shutdown();
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
                logger.error("syncer failed to unregister");
                return;
            }
            catalogContext = null;
            logger.info("unregister catalog name %s", catalogName);
        }
        catch (Throwable t) {
            logger.error(t, "failed to unregister");
        }
    }

    @Override
    public boolean isDefaultCatalog()
    {
        return (readCatalogId() == 0);
    }

    @Override
    public boolean isCatalogReducedResources()
    {
        return (readCatalogId() >= 8);
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
    public int allocReaderId()
    {
        try {
            int readerId = (int) mAllocReaderId.invokeExact();
            if ((readerId >= 0) && (readerId < numWorkerThreads)) {
                return readerId;
            }
        }
        catch (Throwable t) {
            logger.error(t, "failed to allocate query memory");
        }
        throw new RuntimeException("failed to allocate query memory");
    }

    @Override
    public void freeReaderId(int readerId)
    {
        long result = -1;
        try {
            result = (long) mFreeReaderId.invokeExact(readerId);
            if (result > 0) {
                logger.warn("query memory was held too long %d millis", result);
            }
            if (result >= 0) {
                return;
            }
        }
        catch (Throwable t) {
            logger.error(t, "failed to free query memory");
        }
        throw new RuntimeException("failed to free query memory");
    }

    private native long register(long context);
}
