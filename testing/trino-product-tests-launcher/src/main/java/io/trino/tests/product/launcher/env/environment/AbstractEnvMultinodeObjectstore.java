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
package io.trino.tests.product.launcher.env.environment;

import io.trino.tests.product.launcher.docker.DockerFiles;
import io.trino.tests.product.launcher.env.Environment;
import io.trino.tests.product.launcher.env.EnvironmentConfig;
import io.trino.tests.product.launcher.env.EnvironmentProvider;
import io.trino.tests.product.launcher.env.common.Hadoop;
import io.trino.tests.product.launcher.env.common.Minio;
import io.trino.tests.product.launcher.env.common.StandardMultinode;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;

import static io.trino.tests.product.launcher.env.EnvironmentContainers.HADOOP;
import static io.trino.tests.product.launcher.env.common.Minio.MINIO_CONTAINER_NAME;
import static io.trino.tests.product.launcher.env.common.Standard.CONTAINER_TRINO_ETC;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

public abstract class AbstractEnvMultinodeObjectstore
        extends EnvironmentProvider
{
    private static final String S3_BUCKET_NAME = "test-bucket";

    private final String connectorName;
    private final String configDirectoryName;
    private final DockerFiles dockerFiles;
    private final String hadoopImagesVersion;

    public AbstractEnvMultinodeObjectstore(
            String connectorName,
            String configDirectoryName,
            DockerFiles dockerFiles,
            StandardMultinode standardMultinode,
            EnvironmentConfig config,
            Hadoop hadoop,
            Minio minio)
    {
        super(standardMultinode, hadoop, minio);
        this.connectorName = requireNonNull(connectorName, "connectorName is null");
        this.configDirectoryName = requireNonNull(configDirectoryName, "configDirectoryName is null");
        this.dockerFiles = requireNonNull(dockerFiles, "dockerFiles is null");
        this.hadoopImagesVersion = config.getHadoopImagesVersion();
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        // Using hdp3.1 so we are using Hive metastore with version close to versions of hive-*.jars Spark uses
        builder.configureContainer(HADOOP, container -> container.setDockerImageName("ghcr.io/trinodb/testing/hdp3.1-hive:" + hadoopImagesVersion));

        builder.addConnector(
                connectorName,
                forHostPath(dockerFiles.getDockerFilesHostPath("conf/environment/%s/objectstore.properties".formatted(configDirectoryName))),
                CONTAINER_TRINO_ETC + "/catalog/objectstore.properties");

        // Initialize buckets in Minio
        FileAttribute<Set<PosixFilePermission>> posixFilePermissions = PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-r--r--"));
        Path minioBucketDirectory;
        try {
            minioBucketDirectory = Files.createTempDirectory(S3_BUCKET_NAME, posixFilePermissions);
            minioBucketDirectory.toFile().deleteOnExit();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        builder.configureContainer(MINIO_CONTAINER_NAME, container ->
                container.withCopyFileToContainer(forHostPath(minioBucketDirectory), "/data/" + S3_BUCKET_NAME));
    }
}
