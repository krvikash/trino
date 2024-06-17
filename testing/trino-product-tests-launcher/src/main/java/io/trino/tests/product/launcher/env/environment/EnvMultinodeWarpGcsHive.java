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
import io.trino.tests.product.launcher.env.DockerContainer;
import io.trino.tests.product.launcher.env.Environment;
import io.trino.tests.product.launcher.env.common.Hadoop;
import io.trino.tests.product.launcher.env.common.StandardMultinode;
import io.trino.tests.product.launcher.env.common.TestsEnvironment;
import io.trino.tests.product.launcher.testcontainers.PortBinder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Base64;
import java.util.Map;
import java.util.UUID;

import static io.trino.tests.product.launcher.env.EnvironmentContainers.COORDINATOR;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.HADOOP;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.TESTS;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.WORKER;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.attribute.PosixFilePermissions.fromString;
import static org.testcontainers.utility.MountableFile.forHostPath;

@TestsEnvironment
public final class EnvMultinodeWarpGcsHive
        extends MultinodeWarpBase
{
    private static final String gcsTestDirectory = "multinode_warp_gcs_hive_" + UUID.randomUUID();
    private static final String containerGcpCredentialsFile = "/etc/gcp-credentials.json";

    private final DockerFiles dockerFiles;

    @Inject
    public EnvMultinodeWarpGcsHive(
            DockerFiles dockerFiles,
            PortBinder portBinder,
            StandardMultinode standardMultinode,
            Hadoop hadoop)
    {
        super("conf/environment/multinode-warp-gcs-hive", dockerFiles, portBinder, standardMultinode, hadoop);
        this.dockerFiles = dockerFiles;
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        super.extendEnvironment(builder);

        String gcpBase64EncodedCredentials = requireEnv("GCP_CREDENTIALS_KEY");
        byte[] gcpCredentialsBytes = Base64.getDecoder().decode(gcpBase64EncodedCredentials);
        String gcpCredentials = new String(gcpCredentialsBytes, UTF_8);
        Path gcpCredentialsPath = createGcpCredentialsFile(gcpCredentials);

        String gcpStorageBucket = requireEnv("GCP_STORAGE_BUCKET");
        String storageBucket = String.format("gs://%s/%s", gcpStorageBucket, gcsTestDirectory);

        Map<String, String> env = Map.of(
                "GCP_CREDENTIALS", gcpCredentials,
                "GCP_STORAGE_BUCKET", gcpStorageBucket,
                "STORAGE_DIRECTORY", storageBucket);

        builder.configureContainer(HADOOP, container -> configureHadoop(container, gcpCredentialsPath, gcpStorageBucket));
        builder.configureContainer(COORDINATOR, container -> configureContainer(container, env, gcpCredentialsPath));
        builder.configureContainer(WORKER, container -> configureContainer(container, env, gcpCredentialsPath));
        builder.configureContainer(TESTS, container -> container.withEnv(env));
    }

    private Path createGcpCredentialsFile(String gcpCredentials)
    {
        try {
            Path gcpCredentialsJson = Files.createTempFile("gcp-credentials", ".json", PosixFilePermissions.asFileAttribute(fromString("rw-r--r--")));
            Files.writeString(gcpCredentialsJson, gcpCredentials);
            gcpCredentialsJson.toFile().deleteOnExit();
            return gcpCredentialsJson;
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void configureContainer(DockerContainer container, Map<String, String> env, Path gcpCredentialsPath)
    {
        container.withPrivilegedMode(true)
                .withEnv(env)
                .withCopyFileToContainer(forHostPath(gcpCredentialsPath), containerGcpCredentialsFile);
    }

    private void configureHadoop(DockerContainer container, Path gcpCredentialsPath, String gcpStorageBucket)
    {
        container.withPrivilegedMode(true)
                .withCopyFileToContainer(forHostPath(gcpCredentialsPath), containerGcpCredentialsFile)
                .withCopyFileToContainer(forHostPath(getCoreSiteOverrideXml()), "/etc/hadoop/conf/core-site.xml")
                .withCopyFileToContainer(forHostPath(getHiveSiteOverrideXml(gcpStorageBucket)), "/etc/hadoop/conf/hive-site.xml");
    }

    private Path getCoreSiteOverrideXml()
    {
        try {
            String coreSite = Files.readString(dockerFiles.getDockerFilesHostDirectory("conf/environment/multinode-warp-gcs-hive").getPath("core-site.xml"))
                    .replace("%GCP_CREDENTIALS_FILE_PATH%", containerGcpCredentialsFile);
            Path coreSiteXml = Files.createTempFile("core-site", ".xml", PosixFilePermissions.asFileAttribute(fromString("rwxrwxrwx")));
            coreSiteXml.toFile().deleteOnExit();
            Files.writeString(coreSiteXml, coreSite);
            return coreSiteXml;
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Path getHiveSiteOverrideXml(String gcpStorageBucket)
    {
        try {
            String hiveSite = Files.readString(dockerFiles.getDockerFilesHostDirectory("conf/environment/multinode-warp-gcs-hive").getPath("hive-site.xml"))
                    .replace("%GCP_STORAGE_BUCKET%", gcpStorageBucket)
                    .replace("%GCP_WAREHOUSE_DIR%", gcsTestDirectory);
            Path hiveSiteXml = Files.createTempFile("hive-site", ".xml", PosixFilePermissions.asFileAttribute(fromString("rwxrwxrwx")));
            hiveSiteXml.toFile().deleteOnExit();
            Files.writeString(hiveSiteXml, hiveSite);
            return hiveSiteXml;
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
