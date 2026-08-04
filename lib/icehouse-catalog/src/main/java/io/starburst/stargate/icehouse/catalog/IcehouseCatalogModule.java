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
package io.starburst.stargate.icehouse.catalog;

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.starburst.stargate.icehouse.catalog.glue.GlueTableOperationModule;
import io.starburst.stargate.icehouse.catalog.hms.HmsTableOperationModule;
import io.starburst.stargate.icehouse.catalog.rest.RestTableOperationModule;
import io.trino.FeaturesConfig;
import io.trino.metadata.TypeRegistry;
import io.trino.plugin.iceberg.IcebergConfig.VariantMapping;
import io.trino.plugin.iceberg.IcebergTypeManager;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeOperators;
import io.trino.type.InternalTypeManager;

/**
 * Module for external catalog support (Glue, HMS, REST) in Galaxy/SEP deployments.
 *
 * <p>Installs the per-backend modules that provide the shared, deployment-neutral
 * pieces of each backend (client factory configs, telemetry interceptors). The
 * actual {@link IcehouseCatalogFactory} bindings for the new SPI are added by
 * deployment-specific modules — Galaxy wires the factories through
 * {@code GalaxyIcehouseCatalogModule} in {@code icehouse-common}; an SEP
 * deployment would provide its own equivalent.
 */
public class IcehouseCatalogModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        install(new GlueTableOperationModule());
        install(new HmsTableOperationModule());
        install(new RestTableOperationModule());
    }

    @Provides
    @Singleton
    public TypeManager provideTypeManager()
    {
        return new IcebergTypeManager(
                new InternalTypeManager(new TypeRegistry(new TypeOperators(), new FeaturesConfig())),
                VariantMapping.VARIANT);
    }
}
