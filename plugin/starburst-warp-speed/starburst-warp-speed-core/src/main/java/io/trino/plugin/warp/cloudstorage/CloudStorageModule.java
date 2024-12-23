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
package io.trino.plugin.warp.cloudstorage;

import com.google.inject.Binder;
import com.google.inject.ConfigurationException;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.MapBinder;
import io.airlift.configuration.ConfigurationFactory;
import io.airlift.log.Logger;
import io.trino.filesystem.manager.FileSystemConfig;
import io.trino.plugin.warp.cloudstorage.azure.AzureCloudStorage;
import io.trino.plugin.warp.cloudstorage.azure.AzureCloudStorageModule;
import io.trino.plugin.warp.cloudstorage.gcs.GcsCloudStorage;
import io.trino.plugin.warp.cloudstorage.gcs.GcsCloudStorageModule;
import io.trino.plugin.warp.cloudstorage.hdfs.HdfsCloudStorage;
import io.trino.plugin.warp.cloudstorage.hdfs.HdfsCloudStorageModule;
import io.trino.plugin.warp.cloudstorage.s3.S3CloudStorage;
import io.trino.plugin.warp.cloudstorage.s3.S3CloudStorageModule;
import io.trino.plugin.warp.cloudvendors.config.StoreType;
import io.trino.spi.connector.ConnectorContext;

import java.lang.annotation.Annotation;
import java.util.Map;
import java.util.Optional;

import static com.google.inject.multibindings.MapBinder.newMapBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

public class CloudStorageModule
        implements Module
{
    private static final Logger logger = Logger.get(CloudStorageModule.class);

    private final String catalogName;
    private final ConnectorContext context;
    private final ConfigurationFactory configFactory;
    private final StoreType storeType;
    private final Class<? extends Annotation> annotation;

    public CloudStorageModule(String catalogName,
                              ConnectorContext context,
                              ConfigurationFactory configFactory,
                              StoreType storeType,
                              Class<? extends Annotation> annotation)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.context = requireNonNull(context, "context is null");
        this.configFactory = requireNonNull(configFactory, "configFactory is null");
        this.storeType = requireNonNull(storeType, "storeType is null");
        this.annotation = requireNonNull(annotation, "annotation is null");
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(ConfigurationFactory.class).toInstance(configFactory);

        configBinder(binder).bindConfig(FileSystemConfig.class);
        FileSystemConfig config = configFactory.build(FileSystemConfig.class);

        logger.info("annotation %s isHadoopEnabled %s isNativeS3Enabled %s", annotation.toString(), config.isHadoopEnabled(), config.isNativeS3Enabled());
        Injector injector = Guice.createInjector(
                new HdfsCloudStorageModule(catalogName, context, configFactory, annotation, config.isHadoopEnabled()),
                binder1 -> {
                    MapBinder<String, CloudStorage> cloudStorageMap = newMapBinder(binder1, String.class, CloudStorage.class, annotation);

                    switch (storeType) {
                        case S3 -> {
                            binder1.install(new S3CloudStorageModule(context, configFactory, annotation));
                            Key<S3CloudStorage> s3CloudStorageKey = Key.get(S3CloudStorage.class, annotation);
                            cloudStorageMap.addBinding("s3").to(s3CloudStorageKey);
                            cloudStorageMap.addBinding("s3a").to(s3CloudStorageKey);
                            cloudStorageMap.addBinding("s3n").to(s3CloudStorageKey);
                        }
                        case AZURE -> {
                            binder1.install(new AzureCloudStorageModule(context, configFactory, annotation));
                            Key<AzureCloudStorage> azureCloudStorageKey = Key.get(AzureCloudStorage.class, annotation);
                            cloudStorageMap.addBinding("abfs").to(azureCloudStorageKey);
                            cloudStorageMap.addBinding("abfss").to(azureCloudStorageKey);
                        }
                        case GS -> {
                            binder1.install(new GcsCloudStorageModule(configFactory, annotation));
                            Key<GcsCloudStorage> gcsCloudStorageKey = Key.get(GcsCloudStorage.class, annotation);
                            cloudStorageMap.addBinding("gs").to(gcsCloudStorageKey);
                        }
                        case LOCAL -> {}
                    }
                });

        HdfsCloudStorage hdfsCloudStorage = null;
        try {
            hdfsCloudStorage = injector.getInstance(Key.get(HdfsCloudStorage.class, annotation));
        }
        catch (ConfigurationException ignored) {
            // do nothing
        }
        Map<String, CloudStorage> cloudStorageMap = injector.getInstance(Key.get(new TypeLiteral<>(){}, annotation));
        SwitchingCloudStorage switchingCloudStorage = new SwitchingCloudStorage(Optional.ofNullable(hdfsCloudStorage), cloudStorageMap);
        binder.bind(CloudStorage.class).annotatedWith(annotation).toInstance(switchingCloudStorage);
    }
}
