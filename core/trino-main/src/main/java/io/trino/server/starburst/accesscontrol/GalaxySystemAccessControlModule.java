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
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.server.starburst.GalaxyEnabledConfig;
import io.trino.spi.security.SystemAccessControlFactory;

import static com.google.inject.Scopes.SINGLETON;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class GalaxySystemAccessControlModule
        extends AbstractConfigurationAwareModule
{
    private final Class<? extends SystemAccessControlFactory> systemAccessControlFactory;

    public GalaxySystemAccessControlModule()
    {
        this(GalaxyTrinoSystemAccessFactory.class);
    }

    public GalaxySystemAccessControlModule(Class<? extends SystemAccessControlFactory> systemAccessControlFactory)
    {
        this.systemAccessControlFactory = requireNonNull(systemAccessControlFactory, "systemAccessControlFactory is null");
    }

    @Override
    protected void setup(Binder binder)
    {
        install(new GalaxySystemAccessFactoryModule(systemAccessControlFactory));

        configBinder(binder).bindConfig(GalaxyEnabledConfig.class);
        binder.bind(LazyGalaxyAccessControllerSupplier.class).in(SINGLETON);
        binder.bind(GalaxyAccessControllerSupplier.class).to(LazyGalaxyAccessControllerSupplier.class);
        binder.bind(GalaxyAccountPermissionsCache.class).in(SINGLETON);
        newExporter(binder).export(GalaxyAccountPermissionsCache.class).withGeneratedName();
        binder.bind(StarburstAnalyzerHelperImpl.class).in(SINGLETON);
        newOptionalBinder(binder, StarburstAnalyzerHelper.class).setBinding().to(StarburstAnalyzerHelperImpl.class);
    }
}
