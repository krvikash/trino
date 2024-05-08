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
import io.trino.Session;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.security.LocationAccessControl;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestLocationAccessControl
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog("mock")
                .setSchema("default")
                .build();
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session)
                .setWorkerCount(0) // Coordinator only is perfectly enough until we do parallel Stargate connector
                .build();
        queryRunner.installPlugin(new MockConnectorPlugin(MockConnectorFactory.builder()
                .withTableProperties(() -> ImmutableList.of(stringProperty("location", "table location", null, false)))
                .withGetTableHandle((_, _) -> null)
                .withLocationAccessControl(Optional.of(new LocationAccessControl()
                {
                    @Override
                    public void checkCanUseLocation(ConnectorIdentity identity, String location, String queryI)
                    {
                        if (location.contains("invalid_path")) {
                            throw new IllegalArgumentException("Can't access this path: " + location);
                        }
                    }
                }))
                .build()));
        queryRunner.createCatalog("mock", "mock");
        return queryRunner;
    }

    @Test
    public void testCreateTableFailsWhenLocationIsForbidden()
    {
        assertThatThrownBy(() -> getQueryRunner().execute("CREATE TABLE test_table (id INTEGER) WITH (location = 's3://mybucket/invalid_path/test_table')"))
                .hasMessageMatching("Can't access this path:.*");
    }

    @Test
    public void testCreateTableSucceedsWhenLocationIsAllowed()
    {
        assertUpdate("CREATE TABLE test_table (id INTEGER) WITH (location = 's3://mybucket/valid_path/test_table')");
    }
}
