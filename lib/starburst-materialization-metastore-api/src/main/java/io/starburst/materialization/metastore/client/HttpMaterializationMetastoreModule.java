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
package io.starburst.materialization.metastore.client;

import com.google.inject.Binder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.starburst.materialization.metastore.RawMaterializationMetastore;

import static com.google.inject.Scopes.SINGLETON;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;

/**
 * This module provides RawMaterializationMetastore using external REST service.
 * In SEP, it will be provided only if SEP is deployed alongside the sep portal that contains the
 * persistent materialization metastore.
 * In Galaxy, it is going to be provided always, once materialization metastore is migrated and deployed there.
 */
public class HttpMaterializationMetastoreModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(MaterializationMetastoreClientConfig.class);
        httpClientBinder(binder).bindHttpClient("materialization-metastore", ForMaterializationMetastoreClient.class);
        newOptionalBinder(binder, RawMaterializationMetastore.class)
                .setBinding().to(HttpMaterializationMetastore.class).in(SINGLETON);
    }
}
