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
import java.util.Map;
import java.util.UUID;

import static io.trino.tests.product.launcher.env.EnvironmentContainers.COORDINATOR;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.HADOOP;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.TESTS;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.WORKER;
import static java.nio.file.attribute.PosixFilePermissions.fromString;
import static org.testcontainers.utility.MountableFile.forHostPath;

@TestsEnvironment
public final class EnvMultinodeWarpAzureHive
        extends MultinodeWarpBase
{
    private static final String testDirectory = "multinode_warp_azure_hive_" + UUID.randomUUID();

    private final DockerFiles dockerFiles;

    @Inject
    public EnvMultinodeWarpAzureHive(
            DockerFiles dockerFiles,
            PortBinder portBinder,
            StandardMultinode standardMultinode,
            Hadoop hadoop)
    {
        super("conf/environment/multinode-warp-azure-hive", dockerFiles, portBinder, standardMultinode, hadoop);
        this.dockerFiles = dockerFiles;
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        super.extendEnvironment(builder);

        String abfsContainer = requireEnv("ABFS_CONTAINER");
        String abfsAccount = requireEnv("ABFS_ACCOUNT");
        String abfsAccessKey = requireEnv("ABFS_ACCESS_KEY");
        String storageBucket = String.format("abfs://%s@%s.dfs.core.windows.net/%s", abfsContainer, abfsAccount, testDirectory);

        Map<String, String> env = Map.of(
                "ABFS_CONTAINER", abfsContainer,
                "ABFS_ACCOUNT", abfsAccount,
                "ABFS_ACCESS_KEY", abfsAccessKey,
                "STORAGE_DIRECTORY", storageBucket);

        builder.configureContainer(HADOOP, container -> configureHadoop(container, abfsAccount, abfsAccessKey));
        builder.configureContainer(COORDINATOR, container -> container.withEnv(env));
        builder.configureContainer(WORKER, container -> container.withEnv(env));
        builder.configureContainer(TESTS, container -> container.withEnv(env));
    }

    private void configureHadoop(DockerContainer container, String abfsAccount, String abfsAccessKey)
    {
        container.withCopyFileToContainer(forHostPath(getCoreSiteOverrideXml(abfsAccount, abfsAccessKey)), "/etc/hadoop/conf/core-site.xml");
    }

    private Path getCoreSiteOverrideXml(String abfsAccount, String abfsAccessKey)
    {
        try {
            String coreSite = Files.readString(dockerFiles.getDockerFilesHostDirectory("conf/environment/multinode-warp-azure-hive").getPath("core-site.xml"))
                    .replace("%ABFS_ACCOUNT%", abfsAccount)
                    .replace("%ABFS_ACCESS_KEY%", abfsAccessKey);
            Path coreSiteXml = Files.createTempFile("core-site", ".xml", PosixFilePermissions.asFileAttribute(fromString("rwxrwxrwx")));
            coreSiteXml.toFile().deleteOnExit();
            Files.writeString(coreSiteXml, coreSite);
            return coreSiteXml;
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
