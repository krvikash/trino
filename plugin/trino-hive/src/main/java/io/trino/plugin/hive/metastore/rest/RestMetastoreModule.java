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
package io.trino.plugin.hive.metastore.rest;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.metastore.RawHiveMetastoreFactory;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.plugin.hive.AllowHiveTableRename;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;

public class RestMetastoreModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(RestHiveMetastoreConfig.class);
        binder.bind(HiveMetastoreFactory.class).annotatedWith(RawHiveMetastoreFactory.class).to(RestHiveMetastoreFactory.class).in(Scopes.SINGLETON);
        binder.bind(boolean.class).annotatedWith(AllowHiveTableRename.class).toInstance(true);
        httpClientBinder(binder).bindHttpClient("rest-metastore", ForRestMetastore.class);
        newSetBinder(binder, SessionPropertiesProvider.class)
                .addBinding().to(RestMetastoreSessionProperties.class).in(Scopes.SINGLETON);
    }
}
