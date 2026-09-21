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
package io.trino.security;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.base.security.FileBasedSystemAccessControl;
import io.trino.plugin.base.security.ReadOnlySystemAccessControl;
import io.trino.plugin.base.security.testing.TestingSystemAccessControlContext;
import io.trino.plugin.memory.MemoryPlugin;
import io.trino.spi.security.SystemAccessControl;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;

import java.io.File;
import java.net.URISyntaxException;

import static com.google.common.io.Resources.getResource;
import static io.trino.plugin.base.security.FileBasedAccessControlConfig.SECURITY_CONFIG_FILE;
import static io.trino.testing.TestingSession.testSessionBuilder;

final class MultipleSystemAccessControlsQueryRunner
{
    static final String ALPHA = "alpha";
    static final String BETA = "beta";
    static final String GAMMA = "gamma";

    static final String SCHEMA = "default";

    static final String IMPERSONATED_USER = "impersonated_user";

    private MultipleSystemAccessControlsQueryRunner() {}

    static SystemAccessControl fileAccessControl()
            throws URISyntaxException
    {
        String fileRules = new File(getResource("multiple_access_controls_file_rules.json").toURI()).getPath();
        return new FileBasedSystemAccessControl.Factory()
                .create(ImmutableMap.of(SECURITY_CONFIG_FILE, fileRules), new TestingSystemAccessControlContext());
    }

    static SystemAccessControl readOnlyAccessControl()
    {
        return new ReadOnlySystemAccessControl.Factory()
                .create(ImmutableMap.of(), new TestingSystemAccessControlContext());
    }

    static DistributedQueryRunner.Builder<?> builder(SystemAccessControl fileAccessControl, SystemAccessControl readOnlyAccessControl)
    {
        return DistributedQueryRunner.builder(testSessionBuilder()
                        .setCatalog(ALPHA)
                        .setSchema(SCHEMA)
                        .build())
                .setWorkerCount(0)
                .setSystemAccessControls(ImmutableList.of(fileAccessControl, readOnlyAccessControl));
    }

    static void createCatalogs(QueryRunner queryRunner)
    {
        queryRunner.installPlugin(new MemoryPlugin());
        queryRunner.createCatalog(ALPHA, "memory");
        queryRunner.createCatalog(BETA, "memory");
        queryRunner.createCatalog(GAMMA, "memory");
    }
}
