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

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.log.ShapingLoggerFactory;
import io.trino.plugin.warp.util.FailureGeneratorInvocationHandler;

import java.lang.reflect.Proxy;

import static java.util.Objects.requireNonNull;

@Singleton
public class WarpPageSinkFactory
{
    private final FailureGeneratorInvocationHandler failureGeneratorInvocationHandler;
    private final StorageWriterService storageWriterService;
    private final GlobalConfig globalConfig;
    private final ShapingLoggerFactory shapingLoggerFactory;

    @Inject
    public WarpPageSinkFactory(
            FailureGeneratorInvocationHandler failureGeneratorInvocationHandler,
            StorageWriterService storageWriterService,
            GlobalConfig globalConfig,
            ShapingLoggerFactory shapingLoggerFactory)
    {
        this.failureGeneratorInvocationHandler = requireNonNull(failureGeneratorInvocationHandler);
        this.storageWriterService = requireNonNull(storageWriterService);
        this.globalConfig = requireNonNull(globalConfig);
        this.shapingLoggerFactory = requireNonNull(shapingLoggerFactory);
    }

    public PageSink create(StorageWriterSplitConfig storageWriterSplitConfig)
    {
        PageSink pageSink = new WarpPageSink(storageWriterService, storageWriterSplitConfig, shapingLoggerFactory);

        if (globalConfig.isFailureGeneratorEnabled()) {
            pageSink = (PageSink) Proxy.newProxyInstance(
                    pageSink.getClass().getClassLoader(),
                    new Class<?>[] {PageSink.class},
                    failureGeneratorInvocationHandler.getMethodInvocationHandler(pageSink));
        }
        return pageSink;
    }
}
