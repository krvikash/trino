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
package io.trino.plugin.objectstore;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.base.util.AutoCloseableCloser;
import io.trino.plugin.hive.containers.HiveHadoop;
import org.testcontainers.containers.Network;

import java.net.URI;
import java.util.Map;

import static io.trino.testing.containers.TestContainers.getPathFromClassPathResource;
import static org.testcontainers.containers.Network.newNetwork;

public class HiveMinioStorage
        implements AutoCloseable
{
    private final HiveHadoop hiveHadoop;
    private final MinioStorage minioStorage;

    private final AutoCloseableCloser closer = AutoCloseableCloser.create();

    public HiveMinioStorage(String bucketName)
    {
        this(bucketName, HiveHadoop.HIVE3_IMAGE);
    }

    private HiveMinioStorage(String bucketName, String hiveHadoopImage)
    {
        this(bucketName, ImmutableMap.of("/etc/hadoop/conf/core-site.xml", getPathFromClassPathResource("hive_minio_datalake/hive-core-site.xml")), hiveHadoopImage);
    }

    public HiveMinioStorage(String bucketName, Map<String, String> hiveHadoopFilesToMount, String hiveHadoopImage)
    {
        Network network = closer.register(newNetwork());
        HiveHadoop.Builder hiveHadoopBuilder = HiveHadoop.builder()
                .withImage(hiveHadoopImage)
                .withNetwork(network)
                .withFilesToMount(hiveHadoopFilesToMount);
        this.hiveHadoop = closer.register(hiveHadoopBuilder.build());
        this.minioStorage = closer.register(new MinioStorage(bucketName, network));
    }

    public void start()
    {
        minioStorage.start();
        hiveHadoop.start();
    }

    public MinioStorage minioStorage()
    {
        return minioStorage;
    }

    public URI hiveMetastoreEndpoint()
    {
        return hiveHadoop.getHiveMetastoreEndpoint();
    }

    @Override
    public void close()
            throws Exception
    {
        closer.close();
    }
}
