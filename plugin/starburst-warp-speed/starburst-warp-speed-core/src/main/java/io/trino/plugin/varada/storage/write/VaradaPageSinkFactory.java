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
package io.trino.plugin.varada.storage.write;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.trino.plugin.varada.config.GlobalConfig;
import io.trino.plugin.varada.util.FailureGeneratorInvocationHandler;

import java.lang.reflect.Proxy;

import static java.util.Objects.requireNonNull;

@Singleton
public class VaradaPageSinkFactory
{
    private final StorageWriterService storageWriterService;
    private final GlobalConfig globalConfig;
    private final FailureGeneratorInvocationHandler failureGeneratorInvocationHandler;

    @Inject
    public VaradaPageSinkFactory(FailureGeneratorInvocationHandler failureGeneratorInvocationHandler,
            StorageWriterService storageWriterService,
            GlobalConfig globalConfig)
    {
        this.storageWriterService = requireNonNull(storageWriterService);
        this.globalConfig = requireNonNull(globalConfig);
        this.failureGeneratorInvocationHandler = requireNonNull(failureGeneratorInvocationHandler);
    }

    public PageSink create(StorageWriterSplitConfig storageWriterSplitConfig)
    {
        PageSink pageSink = new VaradaPageSink(storageWriterService, storageWriterSplitConfig);

        if (globalConfig.isFailureGeneratorEnabled()) {
            pageSink = (PageSink) Proxy.newProxyInstance(pageSink.getClass().getClassLoader(),
                    new Class<?>[] {PageSink.class},
                    failureGeneratorInvocationHandler.getMethodInvocationHandler(pageSink));
        }
        return pageSink;
    }
}
