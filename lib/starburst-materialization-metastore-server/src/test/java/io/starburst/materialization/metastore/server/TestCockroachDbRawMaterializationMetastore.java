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
package io.starburst.materialization.metastore.server;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.cockroachdb.CockroachContainer;
import org.testcontainers.containers.JdbcDatabaseContainer;

import java.util.Map;

import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestCockroachDbRawMaterializationMetastore
        extends AbstractRawMaterializationMetastoreTest
{
    @Override
    protected JdbcDatabaseContainer<?> createDbContainer()
    {
        // The image's init script exits when its "database exists" probe times out while the node is still starting
        return new CockroachContainer("cockroachdb/cockroach:v24.1.5")
                .withStartupAttempts(3);
    }

    @Override
    protected Map<String, String> serverProperties()
    {
        // CockroachDB connects through the PostgreSQL JDBC URL, so the dialect cannot be inferred
        return ImmutableMap.of("metastore.db.type", "COCKROACHDB");
    }
}
