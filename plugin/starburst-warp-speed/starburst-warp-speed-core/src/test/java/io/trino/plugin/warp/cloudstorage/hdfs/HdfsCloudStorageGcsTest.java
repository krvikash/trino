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
import com.google.inject.Module;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.configuration.ConfigurationFactory;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.manager.FileSystemModule;
import io.trino.plugin.warp.cloudstorage.CloudStorageAbstractTest;
import io.trino.testing.TestingConnectorContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;

@Disabled
public class HdfsCloudStorageGcsTest
        extends CloudStorageAbstractTest
{
    public static final String GCP_CREDENTIAL_KEY = "";
    public static final String BUCKET_NAME = "siactestdata";
    public static final String EMPTY_BUCKET_NAME = "yaffi-test";

    private static Injector injector;

    @BeforeAll
    static void beforeAll()
    {
        injector = Guice.createInjector(new HdfsTestModule());
    }

    @BeforeEach
    void setUp()
    {
        TrinoFileSystemFactory fileSystemFactory = injector.getInstance(TrinoFileSystemFactory.class);

        cloudStorage = new HdfsCloudStorage(fileSystemFactory);
    }

    @Override
    protected String getBucket()
    {
        return "gs://" + BUCKET_NAME + "/";
    }

    @Override
    protected String getEmptyBucket()
    {
        return "gs://" + EMPTY_BUCKET_NAME + "/";
    }

    @Override
    protected String getNotExistBucket()
    {
        return "gs://" + BUCKET_NAME + "-not-exist/";
    }

    static class HdfsTestModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            String catalogName = "catalogName";

            Map<String, String> properties = new HashMap<>();
            properties.put("hive.gcs.use-access-token", "false");
            properties.put("hive.gcs.json-key", GCP_CREDENTIAL_KEY);

            ConfigurationFactory configFactory = new ConfigurationFactory(properties);
            binder.bind(ConfigurationFactory.class).toInstance(configFactory);

            FileSystemModule fileSystemModule = new FileSystemModule(catalogName, new TestingConnectorContext(), false, false);
            fileSystemModule.setConfigurationFactory(configFactory);
            binder.install(fileSystemModule);

            LifeCycleManager lifeCycleManager = Mockito.mock(LifeCycleManager.class);
            binder.bind(LifeCycleManager.class).toInstance(lifeCycleManager);
        }
    }
}
