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
import io.trino.tests.product.launcher.env.common.StandardMultinode;
import io.trino.tests.product.launcher.env.common.TestsEnvironment;
import io.trino.tests.product.launcher.testcontainers.PortBinder;

import static io.trino.tests.product.launcher.env.EnvironmentContainers.COORDINATOR;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.WORKER;
import static io.trino.tests.product.launcher.env.common.Standard.CONTAINER_TRINO_CONFIG_PROPERTIES;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

@TestsEnvironment
public final class EnvMultinodeWarpGlueHive
        extends MultinodeWarpGlueBase
{
    private static final String CONF_ENVIRONMENT_MULTINODE_WARP_GLUE_HIVE = "conf/environment/multinode-warp-glue-hive";
    private final DockerFiles dockerFiles;

    @Inject
    public EnvMultinodeWarpGlueHive(
            DockerFiles dockerFiles,
            PortBinder portBinder,
            StandardMultinode standardMultinode)
    {
        super(CONF_ENVIRONMENT_MULTINODE_WARP_GLUE_HIVE, dockerFiles, portBinder, standardMultinode);
        this.dockerFiles = requireNonNull(dockerFiles, "dockerFiles is null");
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        // revert this and the commit when https://starburstdata.atlassian.net/browse/SEP-15506 is fixed
        super.extendEnvironment(builder);
        builder.configureContainer(COORDINATOR, container -> container
                .withCopyFileToContainer(forHostPath(dockerFiles.getDockerFilesHostDirectory(CONF_ENVIRONMENT_MULTINODE_WARP_GLUE_HIVE).getPath("master-config.properties")), CONTAINER_TRINO_CONFIG_PROPERTIES));
        builder.configureContainer(WORKER, container -> container
                .withCopyFileToContainer(forHostPath(dockerFiles.getDockerFilesHostDirectory(CONF_ENVIRONMENT_MULTINODE_WARP_GLUE_HIVE).getPath("worker-config.properties")), CONTAINER_TRINO_CONFIG_PROPERTIES));
    }
}
