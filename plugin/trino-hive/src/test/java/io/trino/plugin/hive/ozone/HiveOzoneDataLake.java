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
package io.trino.plugin.hive.ozone;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.base.util.AutoCloseableCloser;
import io.trino.plugin.hive.containers.HiveHadoop;
import org.testcontainers.containers.Network;

import java.util.Map;

import static io.trino.testing.containers.TestContainers.getPathFromClassPathResource;

public class HiveOzoneDataLake
        implements AutoCloseable
{
    private static final String DEFAULT_HADOOP_BASE_IMAGE = System.getenv().getOrDefault("HADOOP_BASE_IMAGE", "ghcr.io/trinodb/testing/hdp3.1-hive");

    private final HiveHadoop hiveHadoop;
    private final ApacheOzoneContainer apacheOzoneContainer;

    private AutoCloseableCloser closer = AutoCloseableCloser.create();

    public HiveOzoneDataLake()
    {
        this(DEFAULT_HADOOP_BASE_IMAGE, ImmutableMap.of(
                "/etc/hadoop/conf/core-site.xml", getPathFromClassPathResource("com/starburstdata/presto/plugin/hive/ozone/hive-core-site.xml"),
                // ozone-filesystem-hadoop3.jar file has to be placed within the hadoop classpath
                "/usr/hdp/3.1.0.0-78/hadoop/ozone-filesystem-hadoop3-1.4.0.jar", resolvePathToOzoneLibrary()));
    }

    private static String resolvePathToOzoneLibrary()
    {
        try {
            return getPathFromClassPathResource("ozone-filesystem-hadoop3-1.4.0.jar");
        }
        catch (IllegalArgumentException e) {
            throw new IllegalStateException("""
                    Build project with MAVEN.
                    Resource itself is resolved by maven-dependency-plugin.
                    Therefore, it requires the project to be built by MAVEN, not IDE.
                    """, e);
        }
    }

    public HiveOzoneDataLake(String hiveHadoopImage, Map<String, String> hiveHadoopFilesToMount)
    {
        Network network = closer.register(Network.builder()
                .build());
        this.hiveHadoop = closer.register(HiveHadoop.builder()
                .withImage(hiveHadoopImage)
                .withNetwork(network)
                .withFilesToMount(hiveHadoopFilesToMount)
                .build());
        hiveHadoop.start();

        this.apacheOzoneContainer = closer.register(new ApacheOzoneContainer(network));
        apacheOzoneContainer.start();
    }

    @Override
    public void close()
            throws Exception
    {
        closer.close();
        closer = null;
    }

    public ApacheOzoneContainer getApacheOzoneContainer()
    {
        return apacheOzoneContainer;
    }

    public HiveHadoop getHiveHadoop()
    {
        return hiveHadoop;
    }
}
