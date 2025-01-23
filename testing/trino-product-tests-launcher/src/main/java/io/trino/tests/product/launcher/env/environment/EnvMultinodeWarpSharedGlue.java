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
import io.trino.tests.product.launcher.env.common.StandardMultinode;
import io.trino.tests.product.launcher.env.common.TestsEnvironment;
import io.trino.tests.product.launcher.testcontainers.PortBinder;

import static io.trino.tests.product.launcher.env.EnvironmentContainers.COORDINATOR;

@TestsEnvironment
public final class EnvMultinodeWarpSharedGlue
        extends MultinodeWarpGlueBase
{
    @Inject
    public EnvMultinodeWarpSharedGlue(
            DockerFiles dockerFiles,
            PortBinder portBinder,
            StandardMultinode standardMultinode)
    {
        super("conf/environment/multinode-warp-shared-glue", dockerFiles, portBinder, standardMultinode);
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        super.extendEnvironment(builder);

        builder.configureContainer(COORDINATOR, this::configureWarpResource);
    }

    private void configureWarpResource(DockerContainer container)
    {
        portBinder.exposePort(container, 8085);
    }
}
