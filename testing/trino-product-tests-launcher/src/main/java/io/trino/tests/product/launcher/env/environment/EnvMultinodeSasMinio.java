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

import com.google.inject.Inject;
import io.trino.tests.product.launcher.docker.DockerFiles;
import io.trino.tests.product.launcher.docker.DockerFiles.ResourceProvider;
import io.trino.tests.product.launcher.env.Environment.Builder;
import io.trino.tests.product.launcher.env.EnvironmentProvider;
import io.trino.tests.product.launcher.env.common.Minio;
import io.trino.tests.product.launcher.env.common.StandardMultinode;
import io.trino.tests.product.launcher.env.common.TestsEnvironment;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;

import static io.trino.tests.product.launcher.env.EnvironmentContainers.TESTS;
import static io.trino.tests.product.launcher.env.common.Minio.MINIO_CONTAINER_NAME;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

/**
 * SAS connector reading {@code .sas7bdat} files from S3 (MinIO). The data files are uploaded to
 * the bucket by the product tests themselves, which read them from {@link #CONTAINER_SAS_DATA_DIR}
 * in the tests container.
 */
@TestsEnvironment
public class EnvMultinodeSasMinio
        extends EnvironmentProvider
{
    public static final String S3_BUCKET_NAME = "sas-bucket";
    public static final String CONTAINER_SAS_DATA_DIR = "/docker/sas-data";

    private final ResourceProvider configDir;
    private final ResourceProvider sasDataDir;

    @Inject
    public EnvMultinodeSasMinio(StandardMultinode standardMultinode, Minio minio, DockerFiles dockerFiles)
    {
        super(standardMultinode, minio);
        requireNonNull(dockerFiles, "dockerFiles is null");
        this.configDir = dockerFiles.getDockerFilesHostDirectory("conf/environment/multinode-sas-minio");
        // Reuse the data files of the local-filesystem SAS environment
        this.sasDataDir = dockerFiles.getDockerFilesHostDirectory("conf/environment/multinode-sas");
    }

    @Override
    public void extendEnvironment(Builder builder)
    {
        builder.addConnector("sas", forHostPath(configDir.getPath("sas.properties")));

        // MinIO treats a top-level directory under its data directory as a bucket
        FileAttribute<Set<PosixFilePermission>> posixFilePermissions = PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-r--r--"));
        Path minioBucketDirectory;
        try {
            minioBucketDirectory = Files.createTempDirectory("sas-bucket-contents", posixFilePermissions);
            minioBucketDirectory.toFile().deleteOnExit();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        builder.configureContainer(MINIO_CONTAINER_NAME, container ->
                container.withCopyFileToContainer(forHostPath(minioBucketDirectory), "/data/" + S3_BUCKET_NAME));

        // Objects have to be written through the S3 API, so the tests upload the files from here
        builder.configureContainer(TESTS, container ->
                container.withCopyFileToContainer(forHostPath(sasDataDir.getPath("data")), CONTAINER_SAS_DATA_DIR));
    }
}
