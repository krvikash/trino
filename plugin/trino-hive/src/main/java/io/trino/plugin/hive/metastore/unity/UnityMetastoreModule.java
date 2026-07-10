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

import com.google.common.collect.ImmutableSet;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.log.Logger;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.metastore.RawHiveMetastoreFactory;
import io.trino.plugin.hive.AllowHiveTableRename;
import io.trino.plugin.hive.HiveConfig;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.unitycatalog.client.model.DataSourceFormat.AVRO;
import static io.unitycatalog.client.model.DataSourceFormat.CSV;
import static io.unitycatalog.client.model.DataSourceFormat.DELTA;
import static io.unitycatalog.client.model.DataSourceFormat.JSON;
import static io.unitycatalog.client.model.DataSourceFormat.ORC;
import static io.unitycatalog.client.model.DataSourceFormat.PARQUET;
import static io.unitycatalog.client.model.DataSourceFormat.TEXT;

public class UnityMetastoreModule
        extends AbstractConfigurationAwareModule
{
    private static final Logger log = Logger.get(UnityMetastoreModule.class);

    private final boolean isConfiguredWithHive;

    public UnityMetastoreModule(boolean isConfiguredWithHive)
    {
        this.isConfiguredWithHive = isConfiguredWithHive;
    }

    @Override
    protected void setup(Binder binder)
    {
        log.debug("Configuring through Hive connector: %s", isConfiguredWithHive); // Do not throw as Objectstore connector requires to initialize Hudi connector with Unity metastore
        checkArgument(!buildConfigObject(UnityMetastoreConfig.class).isVendedCredentialsEnabled(), "Setting hive.metastore.unity.vended-credentials-enabled to true is supported only with Delta Lake");
        configBinder(binder).bindConfig(UnityMetastoreConfig.class);
        newOptionalBinder(binder, UnityMetastoreProxyConfig.class);
        if (buildConfigObject(UnityMetastoreConfig.class).isProxyEnabled()) {
            configBinder(binder).bindConfig(UnityMetastoreProxyConfig.class);
        }

        newOptionalBinder(binder, UnityTokenProvider.class)
                .setDefault()
                .to(PersonalAccessTokenProvider.class)
                .in(Scopes.SINGLETON);
        binder.bind(UnityHiveMetastoreFactory.class).in(Scopes.SINGLETON);

        newOptionalBinder(binder, Key.get(HiveMetastoreFactory.class, RawHiveMetastoreFactory.class))
                .setDefault()
                .to(UnityHiveMetastoreFactory.class)
                .in(Scopes.SINGLETON);
        if (buildConfigObject(HiveConfig.class).getDeltaLakeCatalogName().isPresent()) {
            binder.bind(SupportedUnityTableFormatsProvider.class)
                    .toInstance(() -> ImmutableSet.of(PARQUET, AVRO, ORC, CSV, JSON, TEXT, DELTA));
        }
        else {
            binder.bind(SupportedUnityTableFormatsProvider.class)
                    .toInstance(() -> ImmutableSet.of(PARQUET, AVRO, ORC, CSV, JSON, TEXT));
        }
        binder.bind(Key.get(boolean.class, AllowHiveTableRename.class)).toInstance(false);
    }
}
