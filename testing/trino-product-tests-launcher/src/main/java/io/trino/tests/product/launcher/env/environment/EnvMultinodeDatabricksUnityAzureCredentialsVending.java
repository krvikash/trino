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
import io.trino.tests.product.launcher.env.Environment;
import io.trino.tests.product.launcher.env.EnvironmentProvider;
import io.trino.tests.product.launcher.env.common.StandardMultinode;
import io.trino.tests.product.launcher.env.common.TestsEnvironment;

import java.io.File;

import static io.trino.testing.SystemEnvironmentUtils.requireEnv;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.TESTS;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.configureTempto;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.isTrinoContainer;
import static io.trino.tests.product.launcher.env.common.Standard.CONTAINER_TRINO_ETC;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

@TestsEnvironment
public class EnvMultinodeDatabricksUnityAzureCredentialsVending
        extends EnvironmentProvider
{
    private static final File DATABRICKS_JDBC_PROVIDER = new File("testing/trino-product-tests-launcher/target/databricks-jdbc.jar");

    private final DockerFiles.ResourceProvider configDir;

    @Inject
    public EnvMultinodeDatabricksUnityAzureCredentialsVending(StandardMultinode standardMultinode, DockerFiles dockerFiles)
    {
        super(standardMultinode);
        requireNonNull(dockerFiles, "dockerFiles is null");
        configDir = dockerFiles.getDockerFilesHostDirectory("conf/environment/multinode-databricks-unity-azure-credentials-vending");
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        String databricksTestJdbcUrl = requireEnv("AZURE_DATABRICKS_UNITY_JDBC_URL");
        String databricksTestLogin = requireEnv("AZURE_DATABRICKS_LOGIN");
        String databricksTestToken = requireEnv("AZURE_DATABRICKS_TOKEN");

        builder.configureContainers(container -> {
            if (isTrinoContainer(container.getLogicalName())) {
                container.withEnv("AZURE_DATABRICKS_TOKEN", databricksTestToken)
                        .withEnv("AZURE_DATABRICKS_HOST", requireEnv("AZURE_DATABRICKS_HOST"))
                        .withEnv("DATABRICKS_UNITY_CATALOG_NAME", requireEnv("AZURE_DATABRICKS_UNITY_CATALOG_NAME"));
            }
        });

        builder.configureContainer(TESTS, container ->
                container.withEnv("AZURE_DATABRICKS_UNITY_JDBC_URL", databricksTestJdbcUrl)
                        .withEnv("AZURE_DATABRICKS_LOGIN", databricksTestLogin)
                        .withEnv("AZURE_DATABRICKS_TOKEN", databricksTestToken)
                        .withEnv("DATABRICKS_UNITY_CATALOG_NAME", requireEnv("AZURE_DATABRICKS_UNITY_CATALOG_NAME"))
                        .withEnv("DATABRICKS_UNITY_EXTERNAL_LOCATION", requireEnv("AZURE_DATABRICKS_UNITY_EXTERNAL_LOCATION"))
                        .withCopyFileToContainer(
                                forHostPath(DATABRICKS_JDBC_PROVIDER.getAbsolutePath()),
                                "/docker/jdbc/databricks-jdbc.jar"));

        builder.addConnector("hive", forHostPath(configDir.getPath("hive.properties")));
        builder.addConnector(
                "delta_lake",
                forHostPath(configDir.getPath("delta.properties")),
                CONTAINER_TRINO_ETC + "/catalog/delta.properties");
        configureTempto(builder, configDir);
    }
}
