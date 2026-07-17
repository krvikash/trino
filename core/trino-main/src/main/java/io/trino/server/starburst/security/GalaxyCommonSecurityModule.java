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
package io.trino.server.starburst.security;

import com.google.inject.Binder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.execution.PrivilegeUtilitiesApi;
import io.trino.execution.starburst.GalaxyPrivilegeUtilities;
import io.trino.metadata.SystemSecurityMetadata;
import io.trino.server.starburst.accesscontrol.GalaxyAccessControlConfig;
import io.trino.server.starburst.accesscontrol.GalaxySecurityMetadata;

import static com.google.inject.Scopes.SINGLETON;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class GalaxyCommonSecurityModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        newOptionalBinder(binder, EntityPropertyManagerApi.class).setBinding().to(EntityPropertyManager.class).in(SINGLETON);
        newOptionalBinder(binder, SystemSecurityMetadata.class).setBinding().to(GalaxySecurityMetadata.class).in(SINGLETON);
        binder.bind(GalaxySecurityMetadata.class).in(SINGLETON);
        newOptionalBinder(binder, PrivilegeUtilitiesApi.class).setBinding().to(GalaxyPrivilegeUtilities.class).in(SINGLETON);

        configBinder(binder).bindConfig(GalaxyAccessControlConfig.class);
    }
}
