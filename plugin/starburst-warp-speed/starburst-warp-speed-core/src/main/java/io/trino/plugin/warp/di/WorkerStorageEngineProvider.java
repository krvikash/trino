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

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import io.trino.plugin.warp.config.NativeConfig;
import io.trino.plugin.warp.config.SharedConfig;
import io.trino.plugin.warp.log.ShapingLoggerFactory;
import io.trino.plugin.warp.storage.engine.ExceptionThrower;
import io.trino.plugin.warp.storage.engine.StorageEngine;
import io.trino.plugin.warp.storage.engine.nativeimpl.NativeLogger;
import io.trino.plugin.warp.storage.engine.nativeimpl.NativeStorageEngine;
import io.trino.plugin.warp.util.FailureGeneratorInvocationHandler;

import java.lang.reflect.Proxy;

import static java.util.Objects.requireNonNull;

@Singleton
public class WorkerStorageEngineProvider
        implements Provider<StorageEngine>
{
    private final SharedConfig sharedConfig;
    private final NativeConfig nativeConfig;
    private final ExceptionThrower exceptionThrower;
    private final NativeLogger nativeLogger;
    private final FailureGeneratorInvocationHandler failureGeneratorInvocationHandler;
    private final ShapingLoggerFactory shapingLoggerFactory;

    private StorageEngine storageEngine;

    @Inject
    public WorkerStorageEngineProvider(
            SharedConfig sharedConfig,
            NativeConfig nativeConfig,
            ExceptionThrower exceptionThrower,
            NativeLogger nativeLogger,
            FailureGeneratorInvocationHandler failureGeneratorInvocationHandler,
            ShapingLoggerFactory shapingLoggerFactory)
    {
        this.sharedConfig = requireNonNull(sharedConfig);
        this.nativeConfig = requireNonNull(nativeConfig);
        this.exceptionThrower = requireNonNull(exceptionThrower);
        this.nativeLogger = requireNonNull(nativeLogger);
        this.failureGeneratorInvocationHandler = requireNonNull(failureGeneratorInvocationHandler);
        this.shapingLoggerFactory = requireNonNull(shapingLoggerFactory);
    }

    @Override
    public synchronized StorageEngine get()
    {
        if (storageEngine == null) {
            storageEngine = new NativeStorageEngine(
                    sharedConfig,
                    nativeConfig,
                    exceptionThrower,
                    nativeLogger,
                    shapingLoggerFactory);

            if (sharedConfig.isFailureGeneratorEnabled()) {
                storageEngine = (StorageEngine) Proxy.newProxyInstance(
                        storageEngine.getClass().getClassLoader(),
                        new Class<?>[] {StorageEngine.class},
                        failureGeneratorInvocationHandler.getMethodInvocationHandler(storageEngine));
            }
        }
        return storageEngine;
    }
}
