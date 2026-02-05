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
import io.trino.tests.product.launcher.env.EnvironmentConfig;
import io.trino.tests.product.launcher.env.common.Hadoop;
import io.trino.tests.product.launcher.env.common.Minio;
import io.trino.tests.product.launcher.env.common.StandardMultinode;
import io.trino.tests.product.launcher.env.common.TestsEnvironment;

@TestsEnvironment
public final class EnvMultinodeStarburstObjectstore
        extends AbstractEnvMultinodeObjectstore
{
    @Inject
    public EnvMultinodeStarburstObjectstore(
            DockerFiles dockerFiles,
            StandardMultinode standardMultinode,
            EnvironmentConfig config,
            Hadoop hadoop,
            Minio minio)
    {
        super(
                "great_lakes",
                "multinode-starburst-objectstore",
                dockerFiles,
                standardMultinode,
                config,
                hadoop,
                minio);
    }
}
