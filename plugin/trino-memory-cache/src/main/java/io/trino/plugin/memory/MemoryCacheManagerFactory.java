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
package io.trino.plugin.memory;

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.trino.plugin.base.jmx.MBeanServerModule;
import io.trino.spi.subquery.cache.SubqueryCacheManager;
import io.trino.spi.subquery.cache.SubqueryCacheManagerContext;
import io.trino.spi.subquery.cache.SubqueryCacheManagerFactory;
import org.weakref.jmx.guice.MBeanModule;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class MemoryCacheManagerFactory
        implements SubqueryCacheManagerFactory
{
    public static final String NAME = "memory-cache";

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public SubqueryCacheManager create(Map<String, String> config, SubqueryCacheManagerContext context)
    {
        requireNonNull(config, "requiredConfig is null");

        // A plugin is not required to use Guice; it is just very convenient
        Bootstrap app = new Bootstrap(
                "io.trino.bootstrap.cache." + getName(),
                new MemoryCacheModule(),
                new MBeanModule(),
                new MBeanServerModule(),
                binder -> binder.bind(SubqueryCacheManagerContext.class).toInstance(context));

        Injector injector = app
                .doNotInitializeLogging()
                .disableSystemProperties()
                .setRequiredConfigurationProperties(config)
                .initialize();

        return injector.getInstance(ConcurrentCacheManager.class);
    }
}
