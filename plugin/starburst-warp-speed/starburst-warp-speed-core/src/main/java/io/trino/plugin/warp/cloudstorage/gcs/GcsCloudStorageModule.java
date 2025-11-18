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
package io.trino.plugin.warp.cloudstorage.gcs;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.configuration.ConfigurationFactory;
import io.trino.filesystem.gcs.GcsAccessTokenAuth;
import io.trino.filesystem.gcs.GcsAuth;
import io.trino.filesystem.gcs.GcsFileSystemConfig;
import io.trino.filesystem.gcs.GcsFileSystemFactory;
import io.trino.filesystem.gcs.GcsServiceAccountModule;
import io.trino.filesystem.gcs.GcsStorageFactory;
import io.trino.spi.security.ConnectorIdentity;

import java.lang.annotation.Annotation;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

public class GcsCloudStorageModule
        implements Module
{
    private final ConfigurationFactory configFactory;
    private final Class<? extends Annotation> annotation;

    public GcsCloudStorageModule(ConfigurationFactory configFactory,
            Class<? extends Annotation> annotation)
    {
        this.configFactory = requireNonNull(configFactory, "configFactory is null");
        this.annotation = requireNonNull(annotation, "annotation is null");
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(ConfigurationFactory.class).toInstance(configFactory);

        configBinder(binder).bindConfig(GcsFileSystemConfig.class);
        binder.bind(GcsStorageFactory.class);
        binder.bind(GcsFileSystemFactory.class);

        GcsFileSystemConfig config = configFactory.build(GcsFileSystemConfig.class);
        switch (config.getAuthType()) {
            case ACCESS_TOKEN -> binder.bind(GcsAuth.class).to(GcsAccessTokenAuth.class).in(Scopes.SINGLETON);
            case SERVICE_ACCOUNT -> binder.install(new GcsServiceAccountModule());
        }

        binder.bind(GcsCloudStorage.class).annotatedWith(annotation).to(GcsCloudStorage.class);
    }

    @Provides
    @Singleton
    public GcsCloudStorage provideGcsCloudStorage(GcsFileSystemFactory fileSystemFactory, GcsStorageFactory storageFactory)
    {
        GcsFileSystemFactory gcsFileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        GcsStorageFactory gcsStorageFactory = requireNonNull(storageFactory, "storageFactory is null");
        return new GcsCloudStorage(gcsFileSystemFactory, gcsStorageFactory.create(ConnectorIdentity.ofUser("warp")));
    }
}
