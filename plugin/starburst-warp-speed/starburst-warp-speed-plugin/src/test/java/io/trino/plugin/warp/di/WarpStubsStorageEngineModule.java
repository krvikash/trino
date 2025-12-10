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
package io.trino.plugin.warp.di;

import com.google.inject.Binder;
import com.google.inject.Module;
import io.trino.plugin.warp.config.NativeConfig;
import io.trino.plugin.warp.config.SharedConfig;
import io.trino.plugin.warp.log.ShapingLoggerFactory;
import io.trino.plugin.warp.storage.engine.ExceptionThrower;
import io.trino.plugin.warp.storage.engine.StorageEngine;
import io.trino.plugin.warp.storage.engine.StorageEngineConstants;
import io.trino.plugin.warp.storage.engine.StubExceptionThrower;
import io.trino.plugin.warp.storage.engine.StubsStorageEngine;
import io.trino.plugin.warp.storage.engine.StubsStorageEngineConstants;
import io.trino.plugin.warp.storage.engine.nativeimpl.NativeStorageStateHandler;
import io.trino.plugin.warp.storage.read.RangeFillerService;
import io.trino.plugin.warp.storage.read.StubsRangeFillerService;
import io.trino.plugin.warp.tools.CatalogNameProvider;
import io.trino.spi.catalog.CatalogName;

/**
 * Module for binding
 * Should only be used for Testing, never be used in Production.
 */
public class WarpStubsStorageEngineModule
        implements Module
{
    private final StorageEngineConstants storageEngineConstants = new StubsStorageEngineConstants(100);
    private final StorageEngine storageEngine = new StubsStorageEngine();
    private final RangeFillerService rangeFillerService = new StubsRangeFillerService();
    private final ExceptionThrower exceptionThrower = new StubExceptionThrower();

    @Override
    public void configure(Binder binder)
    {
        binder.bind(StorageEngine.class).toInstance(storageEngine);
        binder.bind(RangeFillerService.class).toInstance(rangeFillerService);
        binder.bind(StorageEngineConstants.class).toInstance(storageEngineConstants);
        binder.bind(ExceptionThrower.class).toInstance(exceptionThrower);

        NativeStorageStateHandler nativeStorageStateHandler = new NativeStorageStateHandler(
                new NativeConfig(),
                exceptionThrower,
                new CatalogNameProvider("catalogName"),
                storageEngine,
                new ShapingLoggerFactory(new CatalogName("catalog_name"), new SharedConfig()));
        binder.bind(NativeStorageStateHandler.class).toInstance(nativeStorageStateHandler);
    }

    public StorageEngine getStorageEngine()
    {
        return storageEngine;
    }

    public StorageEngineConstants getStorageEngineConstants()
    {
        return storageEngineConstants;
    }
}
