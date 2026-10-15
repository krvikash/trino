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

import com.google.inject.Binder;
import com.google.inject.Inject;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.security.AccessControlManager;
import io.trino.server.starburst.security.ForGalaxyReadWriteSystemAccessControl;
import io.trino.spi.security.SystemAccessControlFactory;

import static com.google.inject.Scopes.SINGLETON;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.server.starburst.accesscontrol.GalaxySystemAccessControlConfig.AccessControlMode.GALAXY;
import static java.util.Objects.requireNonNull;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class GalaxySystemAccessFactoryModule
        extends AbstractConfigurationAwareModule
{
    private final Class<? extends SystemAccessControlFactory> systemAccessControlFactory;

    public GalaxySystemAccessFactoryModule(Class<? extends SystemAccessControlFactory> systemAccessControlFactory)
    {
        this.systemAccessControlFactory = requireNonNull(systemAccessControlFactory, "systemAccessControlFactory is null");
    }

    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(GalaxySystemAccessControlConfig.class);
        if (buildConfigObject(GalaxySystemAccessControlConfig.class).getAccessControlMode() == GALAXY) {
            binder.bind(FunctionScopeResolver.class).to(GalaxyFunctionScopeResolver.class).in(SINGLETON);
            binder.bind(CatalogDdlObserver.class).to(GalaxyCatalogDdlObserver.class).in(SINGLETON);
        }
        else {
            binder.bind(FunctionScopeResolver.class).to(DenyAllFunctionScopeResolver.class).in(SINGLETON);
            binder.bind(CatalogDdlObserver.class).to(StacCatalogDdlObserver.class).in(SINGLETON);
        }

        binder.bind(GalaxyPermissionsCache.class).in(SINGLETON);
        newExporter(binder).export(GalaxyPermissionsCache.class).withGeneratedName();

        binder.bind(SystemAccessControlFactory.class).annotatedWith(ForGalaxyReadWriteSystemAccessControl.class).to(systemAccessControlFactory);
        binder.bind(LazyRegistration.class).asEagerSingleton();
    }

    private static class LazyRegistration
    {
        @Inject
        public LazyRegistration(AccessControlManager accessControlManager, @ForGalaxyReadWriteSystemAccessControl SystemAccessControlFactory factory)
        {
            requireNonNull(accessControlManager, "accessControlManager is null").addSystemAccessControlFactory(factory);
        }
    }
}
