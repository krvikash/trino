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
package io.trino.plugin.warp.log;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.trino.plugin.warp.config.SharedConfig;
import io.trino.spi.catalog.CatalogName;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;

@Singleton
public class ShapingLoggerFactory
{
    private final CatalogName catalogName;
    private final SharedConfig sharedConfig;
    private final Map<Class<?>, ShapingLogger> instances;

    @Inject
    public ShapingLoggerFactory(CatalogName catalogName, SharedConfig sharedConfig)
    {
        this.catalogName = requireNonNull(catalogName);
        this.sharedConfig = requireNonNull(sharedConfig);
        instances = new ConcurrentHashMap<>();
    }

    public ShapingLogger getInstance(Class<?> clazz)
    {
        return getInstance(
                clazz,
                sharedConfig.getShapingLoggerThreshold(),
                sharedConfig.getShapingLoggerDuration(),
                sharedConfig.getShapingLoggerNumberOfSamples());
    }

    public ShapingLogger getInstance(
            Class<?> clazz,
            int threshold,
            Duration duration,
            int numberOfSamplings)
    {
        return getInstance(clazz, threshold, duration, numberOfSamplings, ShapingLogger.MODE.FORMAT);
    }

    public ShapingLogger getInstance(
            Class<?> clazz,
            int threshold,
            Duration duration,
            int numberOfSamplings,
            ShapingLogger.MODE mode)
    {
        return instances.computeIfAbsent(clazz, _ -> {
            Logger logger = Logger.get(clazz);
            return new ShapingLogger(
                    catalogName.toString(),
                    logger,
                    threshold,
                    duration,
                    numberOfSamplings,
                    mode);
        });
    }

    public ShapingLogger getInstance(
            Class clazz,
            Logger logger,
            int threshold,
            Duration duration,
            int numberOfSamplings,
            ShapingLogger.MODE mode)
    {
        return instances.computeIfAbsent(clazz, _ -> new ShapingLogger(
                catalogName.toString(),
                logger,
                threshold,
                duration,
                numberOfSamplings,
                mode));
    }
}
