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
package io.trino.plugin.warp.cloudstorage.hdfs;

import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import io.airlift.bootstrap.LifeCycleModule;
import io.airlift.configuration.ConfigurationFactory;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.manager.FileSystemModule;
import io.trino.spi.connector.ConnectorContext;

import java.lang.annotation.Annotation;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static java.util.Objects.requireNonNull;

public class HdfsCloudStorageModule
        implements Module
{
    private final String catalogName;
    private final ConnectorContext context;
    private final ConfigurationFactory configFactory;
    private final Class<? extends Annotation> annotation;
    private final boolean isHadoopEnabled;

    public HdfsCloudStorageModule(
            String catalogName,
            ConnectorContext context,
            ConfigurationFactory configFactory,
            Class<? extends Annotation> annotation,
            boolean isHadoopEnabled)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.context = requireNonNull(context, "context is null");
        this.configFactory = requireNonNull(configFactory, "configFactory is null");
        this.annotation = requireNonNull(annotation, "annotation is null");
        this.isHadoopEnabled = isHadoopEnabled;
    }

    @Override
    public void configure(Binder binder)
    {
        newOptionalBinder(binder, Key.get(HdfsCloudStorage.class, annotation));

        if (!isHadoopEnabled) {
            return;
        }

        Injector injector = Guice.createInjector(
                binder1 -> {
                    binder1.bind(ConfigurationFactory.class).toInstance(configFactory);

                    binder1.install(new LifeCycleModule("HdfsCloudStorageModule"));

                    FileSystemModule fileSystemModule = new FileSystemModule(catalogName, context, false, false);
                    fileSystemModule.setConfigurationFactory(configFactory);
                    binder1.install(fileSystemModule);
                });

        TrinoFileSystemFactory fileSystemFactory = injector.getInstance(Key.get(TrinoFileSystemFactory.class));
        binder.bind(HdfsCloudStorage.class).annotatedWith(annotation).toInstance(new HdfsCloudStorage(fileSystemFactory));
    }
}
