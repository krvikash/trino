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
import io.trino.tests.product.launcher.env.common.MitmProxy;
import io.trino.tests.product.launcher.env.common.StandardMultinode;
import io.trino.tests.product.launcher.env.common.TestsEnvironment;

import java.io.File;

import static io.trino.testing.SystemEnvironmentUtils.requireEnv;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.TESTS;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.configureTempto;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.isTrinoContainer;
import static io.trino.tests.product.launcher.env.common.MitmProxy.MITMPROXY_CONTAINER_NAME;
import static io.trino.tests.product.launcher.env.common.MitmProxy.MITMPROXY_PASSWORD;
import static io.trino.tests.product.launcher.env.common.MitmProxy.MITMPROXY_PORT;
import static io.trino.tests.product.launcher.env.common.MitmProxy.MITMPROXY_USERNAME;
import static io.trino.tests.product.launcher.env.common.Standard.CONTAINER_TRINO_ETC;
import static io.trino.tests.product.launcher.env.common.Standard.CONTAINER_TRINO_JVM_CONFIG;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

@TestsEnvironment
public class EnvMultinodeDatabricksUnityProxy
        extends EnvironmentProvider
{
    private static final File DATABRICKS_JDBC_PROVIDER = new File("testing/trino-product-tests-launcher/target/databricks-jdbc.jar");

    private final MitmProxy mitmProxy;
    private final DockerFiles.ResourceProvider configDir;

    @Inject
    public EnvMultinodeDatabricksUnityProxy(StandardMultinode standardMultinode, MitmProxy mitmProxy, DockerFiles dockerFiles)
    {
        super(standardMultinode, mitmProxy);
        requireNonNull(dockerFiles, "dockerFiles is null");
        this.mitmProxy = requireNonNull(mitmProxy, "mitmProxy is null");
        configDir = dockerFiles.getDockerFilesHostDirectory("conf/environment/multinode-databricks-unity-proxy");
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        String databricksTestJdbcUrl = requireEnv("DATABRICKS_UNITY_JDBC_URL");
        String databricksTestLogin = requireEnv("DATABRICKS_LOGIN");
        String databricksTestToken = requireEnv("DATABRICKS_TOKEN");
        String awsRegion = requireEnv("AWS_REGION");

        builder.configureContainers(container -> {
            if (isTrinoContainer(container.getLogicalName())) {
                exportAwsCredentials(container)
                        .withEnv("AWS_REGION", awsRegion)
                        .withEnv("DATABRICKS_TOKEN", databricksTestToken)
                        .withEnv("DATABRICKS_HOST", requireEnv("DATABRICKS_HOST"))
                        .withEnv("DATABRICKS_UNITY_CATALOG_NAME", requireEnv("DATABRICKS_UNITY_CATALOG_NAME"))
                        .withEnv("PROXY_HOST", MITMPROXY_CONTAINER_NAME)
                        .withEnv("PROXY_PORT", Integer.toString(MITMPROXY_PORT))
                        .withEnv("PROXY_USERNAME", MITMPROXY_USERNAME)
                        .withEnv("PROXY_PASSWORD", MITMPROXY_PASSWORD)
                        // Setup the mitmproxy certificate
                        .withCopyFileToContainer(forHostPath(mitmProxy.getCertificatePath()), CONTAINER_TRINO_ETC + "/cert/mitmproxy.jks")
                        .withCopyFileToContainer(forHostPath(configDir.getPath("jvm.config")), CONTAINER_TRINO_JVM_CONFIG);
            }
        });

        builder.configureContainer(TESTS, container -> exportAwsCredentials(container)
                .withEnv("DATABRICKS_JDBC_URL", databricksTestJdbcUrl)
                .withEnv("DATABRICKS_LOGIN", databricksTestLogin)
                .withEnv("DATABRICKS_TOKEN", databricksTestToken)
                .withEnv("DATABRICKS_UNITY_CATALOG_NAME", requireEnv("DATABRICKS_UNITY_CATALOG_NAME"))
                .withEnv("DATABRICKS_UNITY_EXTERNAL_LOCATION", requireEnv("DATABRICKS_UNITY_EXTERNAL_LOCATION"))
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
