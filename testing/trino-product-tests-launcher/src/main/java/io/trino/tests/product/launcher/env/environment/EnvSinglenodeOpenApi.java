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
import io.trino.tests.product.launcher.env.DockerContainer;
import io.trino.tests.product.launcher.env.Environment;
import io.trino.tests.product.launcher.env.EnvironmentProvider;
import io.trino.tests.product.launcher.env.common.Standard;
import io.trino.tests.product.launcher.env.common.TestsEnvironment;
import io.trino.tests.product.launcher.testcontainers.PortBinder;
import org.testcontainers.containers.wait.strategy.Wait;

import static io.trino.tests.product.launcher.env.EnvironmentContainers.COORDINATOR;
import static io.trino.tests.product.launcher.env.common.Standard.CONTAINER_TRINO_ETC;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

@TestsEnvironment
public class EnvSinglenodeOpenApi
        extends EnvironmentProvider
{
    private final PortBinder binder;
    private final ResourceProvider configDir;

    @Inject
    public EnvSinglenodeOpenApi(Standard standard, PortBinder binder, DockerFiles dockerFiles)
    {
        super(standard);
        this.binder = requireNonNull(binder, "binder is null");
        requireNonNull(dockerFiles, "dockerFiles is null");
        this.configDir = dockerFiles.getDockerFilesHostDirectory("conf/environment/singlenode-openapi/");
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        DockerContainer server = new DockerContainer("python:3.14-alpine", "apiserver")
                .withCopyFileToContainer(forHostPath(configDir.getPath("server.py")), "/")
                .withCommand("python", "/server.py")
                .withExposedPorts(3000)
                .waitingFor(Wait.forHttp("/health").forPort(3000).forStatusCode(200));
        binder.exposePort(server, 3000);
        builder.addContainer(server);
        builder.configureContainer(COORDINATOR, coordinator ->
                coordinator.withCopyFileToContainer(
                        forHostPath(configDir.getPath("description.json")),
                        CONTAINER_TRINO_ETC + "/description.json"));
        builder.addConnector("starburst_openapi", forHostPath(configDir.getPath("openapi.properties")));
    }
}
