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
package io.trino.server.starburst.accesscontrol;

import com.google.inject.Inject;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.starburst.stargate.accesscontrol.client.TrinoSecurityApi;
import io.trino.server.starburst.catalogs.CatalogResolver;
import io.trino.server.starburst.catalogs.StaticCatalogResolver;
import io.trino.spi.security.SystemAccessControl;
import io.trino.spi.security.SystemAccessControlFactory;

import java.util.Map;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

public class GalaxyTrinoSystemAccessFactory
        implements SystemAccessControlFactory
{
    public static final String NAME = "galaxy";

    private final int backgroundProcessingThreads;
    private final FunctionScopeResolver functionScopeResolver;
    private final int visibilityBatchSize;
    private final GalaxySystemAccessControlConfig.AccessControlMode accessControlMode;
    private final TrinoSecurityApi trinoSecurityApi;
    private final OpenTelemetry openTelemetry;
    private final LazyGalaxyAccessControllerSupplier lazyGalaxyAccessControllerSupplier;
    private final Tracer tracer;

    @Inject
    public GalaxyTrinoSystemAccessFactory(
            GalaxySystemAccessControlConfig systemAccessControlConfig,
            TrinoSecurityApi trinoSecurityApi,
            OpenTelemetry openTelemetry,
            LazyGalaxyAccessControllerSupplier lazyGalaxyAccessControllerSupplier,
            FunctionScopeResolver functionScopeResolver)
    {
        requireNonNull(systemAccessControlConfig, "systemAccessControlConfig is null");
        this.backgroundProcessingThreads = systemAccessControlConfig.getBackgroundProcessingThreads();
        this.functionScopeResolver = requireNonNull(functionScopeResolver, "functionScopeResolver is null");
        this.visibilityBatchSize = systemAccessControlConfig.getVisibilityBatchSize();
        this.accessControlMode = systemAccessControlConfig.getAccessControlMode();
        this.trinoSecurityApi = requireNonNull(trinoSecurityApi, "trinoSecurityApi is null");
        this.openTelemetry = requireNonNull(openTelemetry, "openTelemetry is null");
        this.lazyGalaxyAccessControllerSupplier = requireNonNull(lazyGalaxyAccessControllerSupplier, "lazyGalaxyAccessControllerSupplier is null");
        this.tracer = openTelemetry.getTracer("trino.system-access-control." + NAME);
    }

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public SystemAccessControl create(Map<String, String> properties, SystemAccessControlContext context)
    {
        Bootstrap app = new Bootstrap(
                new JsonModule(),
                binder -> {
                    binder.bind(OpenTelemetry.class).toInstance(openTelemetry);
                    binder.bind(Tracer.class).toInstance(tracer);

                    // TODO GalaxyAccessControlConfig and CatalogIds are bound in main Guice context too, but currently are given different configuration.
                    //  Only here we're provided with `galaxy.read-only-catalogs` values.
                    binder.bind(CatalogResolver.class).to(StaticCatalogResolver.class);
                    configBinder(binder).bindConfig(GalaxyAccessControlConfig.class);
                    configBinder(binder).bindConfig(GalaxyAccessControlUrlsConfig.class);

                    configBinder(binder).bindConfig(GalaxySystemAccessControlConfig.class);
                    binder.bind(TrinoSecurityApi.class).toInstance(trinoSecurityApi);
                });

        app.doNotInitializeLogging()
                .setRequiredConfigurationProperties(properties)
                .initialize();

        CatalogManagementAccessControl catalogManagementAccessControl = switch (accessControlMode) {
            case GALAXY -> new GalaxyCatalogManagementAccessControl();
            case SEP -> new StacCatalogManagementAccessControl(lazyGalaxyAccessControllerSupplier);
        };

        return new GalaxyAccessControl(backgroundProcessingThreads, visibilityBatchSize, lazyGalaxyAccessControllerSupplier, functionScopeResolver, catalogManagementAccessControl);
    }
}
