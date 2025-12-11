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
package io.trino.plugin.warp;

import com.google.inject.Module;
import com.starburstdata.trino.plugin.license.LicenseVerifier;
import io.trino.plugin.warp.dispatcher.DispatcherCacheManagerFactory;
import io.trino.plugin.warp.dispatcher.DispatcherConnectorFactory;
import io.trino.plugin.warp.dispatcher.WarpPluginSharedInstancesFactory;
import io.trino.spi.Plugin;
import io.trino.spi.TrinoException;
import io.trino.spi.cache.CacheManagerFactory;
import io.trino.spi.connector.ConnectorFactory;
import org.weakref.jmx.$internal.guava.annotations.VisibleForTesting;

import java.util.List;

import static io.trino.plugin.warp.WarpErrorCode.WARP_SETUP;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TimeType.TIME_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static java.util.Objects.requireNonNull;

public class WarpPlugin
        implements Plugin
{
    private final LicenseVerifier licenseVerifier;

    private WarpPluginSharedInstancesFactory warpPluginSharedInstancesFactory;
    private com.google.inject.Module storageEngineModule;
    private Module proxyModule;

    public WarpPlugin()
    {
        this(() -> true);
    }

    public WarpPlugin(LicenseVerifier licenseVerifier)
    {
        this.licenseVerifier = requireNonNull(licenseVerifier);
    }

    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        WarpConnectorFactory warpConnectorFactory = new WarpConnectorFactory(
                getSharedInstancesFactory(),
                this.getConnectorFactory(),
                licenseVerifier);
        return List.of(warpConnectorFactory);
    }

    @Override
    public Iterable<CacheManagerFactory> getCacheManagerFactories()
    {
        WarpCacheManagerFactory warpCacheManagerFactory = new WarpCacheManagerFactory(
                getSharedInstancesFactory(),
                this.getCacheManagerFactory());
        return List.of(warpCacheManagerFactory);
    }

    private synchronized WarpPluginSharedInstancesFactory getSharedInstancesFactory()
    {
        if (warpPluginSharedInstancesFactory == null) {
            warpPluginSharedInstancesFactory = new WarpPluginSharedInstancesFactory(storageEngineModule);
        }
        return warpPluginSharedInstancesFactory;
    }

    private DispatcherConnectorFactory getConnectorFactory()
    {
        return new DispatcherConnectorFactory(proxyModule);
    }

    private DispatcherCacheManagerFactory getCacheManagerFactory()
    {
        return new DispatcherCacheManagerFactory();
    }

    @VisibleForTesting
    public WarpPlugin withStorageEngineModule(Module module)
    {
        this.storageEngineModule = module;
        return this;
    }

    @VisibleForTesting
    public WarpPlugin withProxyModule(Module module)
    {
        this.proxyModule = module;
        return this;
    }

    private static void verifyTypeSizes()
    {
        if (BIGINT.getFixedSize() != Long.BYTES) {
            throw new TrinoException(WARP_SETUP, "BIGINT size is not equal to Long size");
        }
        if (BOOLEAN.getFixedSize() != Byte.BYTES) {
            throw new TrinoException(WARP_SETUP, "BOOLEAN size is not equal to Byte size");
        }
        if (DATE.getFixedSize() != Integer.BYTES) {
            throw new TrinoException(WARP_SETUP, "DATE size is not equal to Integer size");
        }
        if (REAL.getFixedSize() != Integer.BYTES) {
            throw new TrinoException(WARP_SETUP, "REAL size is not equal to Integer size");
        }
        if (DOUBLE.getFixedSize() != Double.BYTES) {
            throw new TrinoException(WARP_SETUP, "DOUBLE size is not equal to Double size");
        }
        if (INTEGER.getFixedSize() != Integer.BYTES) {
            throw new TrinoException(WARP_SETUP, "INTEGER size is not equal to Integer size");
        }
        if (TIMESTAMP_MILLIS.getFixedSize() != Long.BYTES) {
            throw new TrinoException(WARP_SETUP, "TIMESTAMP size is not equal to Long size");
        }
        if (TIMESTAMP_TZ_MILLIS.getFixedSize() != Long.BYTES) {
            throw new TrinoException(WARP_SETUP, "TIMESTAMP_WITH_TIME_ZONE size is not equal to Long size");
        }
        if (TIME_MILLIS.getFixedSize() != Long.BYTES) {
            throw new TrinoException(WARP_SETUP, "TIME size is not equal to Long size");
        }
    }

    static {
        verifyTypeSizes();
    }
}
