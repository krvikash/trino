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
package io.trino.filesystem.gcs;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;

import java.util.Optional;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

public class GcsFileSystemModule
        extends AbstractConfigurationAwareModule
{
    private final Optional<String> configPrefix;

    public GcsFileSystemModule()
    {
        this(Optional.empty());
    }

    public GcsFileSystemModule(Optional<String> configPrefix)
    {
        this.configPrefix = requireNonNull(configPrefix, "configPrefix is null");
    }

    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(GcsFileSystemConfig.class, configPrefix.orElse(null));
        binder.bind(GcsStorageFactory.class).in(Scopes.SINGLETON);
        binder.bind(GcsFileSystemFactory.class).in(Scopes.SINGLETON);

        switch (buildConfigObject(GcsFileSystemConfig.class, configPrefix.orElse(null)).getAuthType()) {
            case ACCESS_TOKEN -> binder.bind(GcsAuth.class).to(GcsAccessTokenAuth.class).in(Scopes.SINGLETON);
            case SERVICE_ACCOUNT -> install(new GcsServiceAccountModule(configPrefix));
            case APPLICATION_DEFAULT -> binder.bind(GcsAuth.class).to(ApplicationDefaultAuth.class).in(Scopes.SINGLETON);
        }
    }
}
