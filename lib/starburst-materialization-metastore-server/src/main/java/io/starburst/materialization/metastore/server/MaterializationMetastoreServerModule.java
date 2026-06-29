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
package io.starburst.materialization.metastore.server;

import com.google.inject.Binder;
import io.airlift.configuration.AbstractConfigurationAwareModule;

import static com.google.inject.Scopes.SINGLETON;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static java.util.Objects.requireNonNull;

public class MaterializationMetastoreServerModule
        extends AbstractConfigurationAwareModule
{
    private final Class<? extends MaterializationMetastoreResource> materializationMetastoreResourceClass;

    /**
     * @param materializationMetastoreResourceClass subclass of MaterializationMetastoreResource that contain security enforcing annotation.
     */
    public MaterializationMetastoreServerModule(Class<? extends MaterializationMetastoreResource> materializationMetastoreResourceClass)
    {
        this.materializationMetastoreResourceClass = requireNonNull(materializationMetastoreResourceClass, "materializationMetastoreResourceClass is null");
    }

    @Override
    protected void setup(Binder binder)
    {
        binder.bind(DbRawMaterializationMetastore.class).in(SINGLETON);
        jaxrsBinder(binder).bind(materializationMetastoreResourceClass);
    }
}
