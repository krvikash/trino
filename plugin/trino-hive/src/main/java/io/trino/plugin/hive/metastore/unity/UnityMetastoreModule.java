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
package io.trino.plugin.hive.metastore.unity;

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.metastore.RawHiveMetastoreFactory;
import io.trino.plugin.hive.AllowHiveTableRename;
import io.trino.plugin.hive.security.SecurityConfig;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.hive.security.HiveSecurityModule.HiveSecurity.READ_ONLY;

public class UnityMetastoreModule
        extends AbstractConfigurationAwareModule
{
    private final boolean isConfiguredWithHive;

    public UnityMetastoreModule(boolean isConfiguredWithHive)
    {
        this.isConfiguredWithHive = isConfiguredWithHive;
    }

    @Override
    protected void setup(Binder binder)
    {
        checkArgument(isConfiguredWithHive, "Unity metastore is only supported with Hive");
        SecurityConfig securityConfig = buildConfigObject(SecurityConfig.class);
        checkArgument(securityConfig.getSecuritySystem() == READ_ONLY, "hive.security must be set to READ_ONLY");
        configBinder(binder).bindConfig(UnityMetastoreConfig.class);

        binder.bind(UnityHiveMetastoreFactory.class).in(Scopes.SINGLETON);

        newOptionalBinder(binder, Key.get(HiveMetastoreFactory.class, RawHiveMetastoreFactory.class))
                .setDefault()
                .to(UnityHiveMetastoreFactory.class)
                .in(Scopes.SINGLETON);
        binder.bind(Key.get(boolean.class, AllowHiveTableRename.class)).toInstance(false);
    }
}
