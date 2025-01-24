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

import static java.util.Objects.requireNonNull;

@Singleton
public class NativeConnectorSync
        implements ConnectorSync
{
    private static final Logger logger = Logger.get(NativeConnectorSync.class);
    private static final int ALLOC_ALIGNMENT = Integer.BYTES;

    private final CatalogName catalogName;
    private final EventBus eventBus;
    private MemorySegment catalogContext;

    // syncher API
    private final MethodHandle mGetContextSize;
    private final MethodHandle mRegister;
    private final MethodHandle mUnregister;

    @Inject
    public NativeConnectorSync(
            CatalogName catalogName,
            EventBus eventBus)
    {
        try {
            SymbolLookup libraryHandle = SymbolLookup.loaderLookup();
            Linker linker = Linker.nativeLinker();

            // syncher API
            mGetContextSize = linker.downcallHandle(libraryHandle.find("syncher_get_context_size").orElseThrow(),
                    FunctionDescriptor.of(ValueLayout.JAVA_INT));
            mRegister = linker.downcallHandle(libraryHandle.find("syncher_register").orElseThrow(),
                    FunctionDescriptor.of(ValueLayout.JAVA_BOOLEAN, ValueLayout.ADDRESS));
            mUnregister = linker.downcallHandle(libraryHandle.find("syncher_unregister").orElseThrow(),
                    FunctionDescriptor.of(ValueLayout.JAVA_BOOLEAN, ValueLayout.ADDRESS));

            this.catalogName = catalogName;
            this.eventBus = requireNonNull(eventBus);

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
            // register and get memory address. note that the name is not passed to native. no need.
            boolean success = (boolean) mRegister.invokeExact(catalogContext);
            if (success) {
                // complete the regisgtration
                logger.info("catalog %s registered", catalogName);
                eventBus.post(new ConnectorSyncInitializedEvent(true));
                return;
            }
        }
        catch (Throwable t) {
            logger.error(t, "failed to call register");
        }
        shutdown();
        throw new RuntimeException("failed to register catalog " + catalogName);
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
            logger.info("catalog %s unreigtered", catalogName);
        }
        catch (Throwable t) {
            logger.error(t, "failed to call unregister");
        }
    }
}
