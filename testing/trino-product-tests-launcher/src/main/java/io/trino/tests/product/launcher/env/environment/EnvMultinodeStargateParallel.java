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

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.tests.product.launcher.docker.DockerFiles;
import io.trino.tests.product.launcher.env.DockerContainer;
import io.trino.tests.product.launcher.env.Environment;
import io.trino.tests.product.launcher.env.EnvironmentConfig;
import io.trino.tests.product.launcher.env.EnvironmentProvider;
import io.trino.tests.product.launcher.env.ServerPackage;
import io.trino.tests.product.launcher.env.common.Minio;
import io.trino.tests.product.launcher.env.common.Standard;
import io.trino.tests.product.launcher.env.common.StandardMultinode;
import io.trino.tests.product.launcher.env.common.TestsEnvironment;
import io.trino.tests.product.launcher.env.jdk.JdkProvider;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.tests.product.launcher.env.common.Minio.MINIO_CONTAINER_NAME;
import static io.trino.tests.product.launcher.env.common.Standard.CONTAINER_TRINO_CONFIG_PROPERTIES;
import static io.trino.tests.product.launcher.env.common.Standard.CONTAINER_TRINO_ETC;
import static io.trino.tests.product.launcher.env.common.Standard.createTrinoContainer;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

@TestsEnvironment
public final class EnvMultinodeStargateParallel
        extends EnvironmentProvider
{
    private static final String S3_SPOOLING_BUCKET = "spooling";

    private final DockerFiles dockerFiles;
    private final String imagesVersion;
    private final File serverPackage;
    private final JdkProvider jdkProvider;

    @Inject
    public EnvMultinodeStargateParallel(
            StandardMultinode standardMultinode,
            Minio minio,
            DockerFiles dockerFiles,
            EnvironmentConfig environmentConfig,
            @ServerPackage File serverPackage,
            JdkProvider jdkProvider)
    {
        super(ImmutableList.of(standardMultinode, minio));
        this.dockerFiles = requireNonNull(dockerFiles, "dockerFiles is null");
        this.imagesVersion = requireNonNull(environmentConfig, "environmentConfig is null").getImagesVersion();
        this.serverPackage = requireNonNull(serverPackage, "serverPackage is null");
        this.jdkProvider = requireNonNull(jdkProvider, "jdkProvider is null");
        checkArgument(serverPackage.getName().endsWith(".tar.gz"), "Currently only server .tar.gz package is supported");
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        // Initialize spooling bucket in MinIO
        FileAttribute<Set<PosixFilePermission>> posixFilePermissions = PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-r--r--"));
        Path minioBucketDirectory;
        try {
            minioBucketDirectory = Files.createTempDirectory("test-bucket-contents", posixFilePermissions);
            minioBucketDirectory.toFile().deleteOnExit();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        builder.configureContainer(MINIO_CONTAINER_NAME, container ->
                container.withCopyFileToContainer(forHostPath(minioBucketDirectory), "/data/" + S3_SPOOLING_BUCKET));

        // Local Trino Cluster
        builder.addConnector(
                "stargate_parallel",
                forHostPath(dockerFiles.getDockerFilesHostPath("conf/environment/multinode-stargate-parallel/local/remote_tpch.properties")),
                CONTAINER_TRINO_ETC + "/catalog/remote_tpch.properties");

        // Remote Trino Cluster
        DockerFiles.ResourceProvider remoteTrinoResourceProvider = dockerFiles.getDockerFilesHostDirectory("conf/environment/multinode-stargate-parallel/remote");
        DockerContainer remoteCoordinator =
                createTrinoContainer(dockerFiles, serverPackage, jdkProvider, false, false, "ghcr.io/trinodb/testing/almalinux9-oj17:" + imagesVersion, "remote-trino-coordinator")
                        .withCopyFileToContainer(forHostPath(dockerFiles.getDockerFilesHostPath("common/standard/access-control.properties")), Standard.CONTAINER_TRINO_ACCESS_CONTROL_PROPERTIES)
                        .withCopyFileToContainer(forHostPath(remoteTrinoResourceProvider.getPath("coordinator-config.properties")), Standard.CONTAINER_TRINO_CONFIG_PROPERTIES)
                        .withCopyFileToContainer(forHostPath(remoteTrinoResourceProvider.getPath("spooling-manager.properties")), CONTAINER_TRINO_ETC + "/spooling-manager.properties")
                        .withFixedExposedPort(18080, 8080);
        DockerContainer remoteWorker =
                createTrinoContainer(dockerFiles, serverPackage, jdkProvider, false, false, "ghcr.io/trinodb/testing/almalinux9-oj17:" + imagesVersion, "remote-trino-worker")
                        .withCopyFileToContainer(forHostPath(remoteTrinoResourceProvider.getPath("worker-config.properties")), CONTAINER_TRINO_CONFIG_PROPERTIES)
                        .withCopyFileToContainer(forHostPath(remoteTrinoResourceProvider.getPath("spooling-manager.properties")), CONTAINER_TRINO_ETC + "/spooling-manager.properties");
        builder.addContainer(remoteCoordinator);
        builder.addContainer(remoteWorker);
    }
}
