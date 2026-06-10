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
import io.trino.tests.product.launcher.env.EnvironmentProvider;
import io.trino.tests.product.launcher.env.common.StandardMultinode;
import io.trino.tests.product.launcher.env.common.TestsEnvironment;

import java.io.File;

import static io.trino.testing.SystemEnvironmentUtils.requireEnv;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.TESTS;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.configureTempto;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.isTrinoContainer;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

@TestsEnvironment
public class EnvMultinodeDatabricksUnityIceberg
        extends EnvironmentProvider
{
    private static final File DATABRICKS_JDBC_PROVIDER = new File("testing/trino-product-tests-launcher/target/databricks-jdbc.jar");

    private final DockerFiles.ResourceProvider configDir;

    @Inject
    public EnvMultinodeDatabricksUnityIceberg(StandardMultinode standardMultinode, DockerFiles dockerFiles)
    {
        super(standardMultinode);
        requireNonNull(dockerFiles, "dockerFiles is null");
        configDir = dockerFiles.getDockerFilesHostDirectory("conf/environment/multinode-databricks-unity-iceberg");
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        String databricksTestJdbcUrl = requireEnv("DATABRICKS_UNITY_JDBC_URL");
        String databricksTestLogin = requireEnv("DATABRICKS_LOGIN");
        String databricksTestToken = requireEnv("DATABRICKS_TOKEN");
        String databricksHost = requireEnv("DATABRICKS_HOST");
        String databricksUnityCatalogName = requireEnv("DATABRICKS_UNITY_CATALOG_NAME");
        String awsRegion = requireEnv("AWS_REGION");

        builder.configureContainers(container -> {
            if (isTrinoContainer(container.getLogicalName())) {
                exportAwsCredentials(container)
                        .withEnv("AWS_REGION", awsRegion)
                        .withEnv("DATABRICKS_TOKEN", databricksTestToken)
                        .withEnv("DATABRICKS_HOST", databricksHost)
                        .withEnv("DATABRICKS_UNITY_CATALOG_NAME", databricksUnityCatalogName);
            }
        });

        builder.configureContainer(TESTS, container -> exportAwsCredentials(container)
                .withEnv("DATABRICKS_JDBC_URL", databricksTestJdbcUrl)
                .withEnv("DATABRICKS_LOGIN", databricksTestLogin)
                .withEnv("DATABRICKS_TOKEN", databricksTestToken)
                .withEnv("DATABRICKS_HOST", databricksHost)
                .withEnv("DATABRICKS_UNITY_CATALOG_NAME", databricksUnityCatalogName)
                .withEnv("AWS_REGION", awsRegion)
                .withCopyFileToContainer(
                        forHostPath(DATABRICKS_JDBC_PROVIDER.getAbsolutePath()),
                        "/docker/jdbc/databricks-jdbc.jar"));

        builder.addConnector("iceberg", forHostPath(configDir.getPath("iceberg.properties")));
        configureTempto(builder, configDir);
    }

    private static DockerContainer exportAwsCredentials(DockerContainer container)
    {
        container = exportAwsCredential(container, "TRINO_AWS_ACCESS_KEY_ID", "AWS_ACCESS_KEY_ID");
        container = exportAwsCredential(container, "TRINO_AWS_SECRET_ACCESS_KEY", "AWS_SECRET_ACCESS_KEY");
        return container;
    }

    private static DockerContainer exportAwsCredential(DockerContainer container, String credentialEnvVariable, String containerEnvVariable)
    {
        String credentialValue = System.getenv(credentialEnvVariable);
        if (credentialValue == null) {
            throw new IllegalStateException(format("Environment variable %s not set", credentialEnvVariable));
        }
        return container.withEnv(containerEnvVariable, credentialValue);
    }
}
