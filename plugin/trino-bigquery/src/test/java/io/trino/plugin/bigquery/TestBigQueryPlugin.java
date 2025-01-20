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
package io.trino.plugin.bigquery;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.TestingConnectorContext;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.plugin.bigquery.BigQueryQueryRunner.DUMMY_BIGQUERY_CREDENTIALS_KEY;
import static org.assertj.core.api.Assertions.assertThat;

public class TestBigQueryPlugin
{
    @Test
    public void testCreateConnector()
    {
        BigQueryPlugin plugin = new BigQueryPlugin();
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        assertThat(factory).isInstanceOf(BigQueryConnectorFactory.class);

        Connector connector = factory.create(
                "test",
                Map.of(
                        "bigquery.project-id", "xxx",
                        "bigquery.credentials-key", DUMMY_BIGQUERY_CREDENTIALS_KEY,
                        "bootstrap.quiet", "true"),
                new TestingConnectorContext());
        // MV substitution support must be wired; verifiable without BigQuery/GCP access.
        assertThat(connector.getSubstitutionMetadata()).isNotNull();
        connector.shutdown();
    }

    @Test
    void testGetSecuritySensitivePropertyNames()
    {
        BigQueryPlugin plugin = new BigQueryPlugin();
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        Map<String, String> config = ImmutableMap.of(
                "non-existent-property", "value",
                "bigquery.credentials-key", "key",
                "bigquery.metadata.parallelism", "shouldBeInt",
                "bigquery.projection-pushdown-enabled", "true");

        Set<String> sensitiveProperties = factory.getSecuritySensitivePropertyNames("catalog", config, new TestingConnectorContext());

        assertThat(sensitiveProperties).containsExactlyInAnyOrder("non-existent-property", "bigquery.credentials-key");
    }

    @Test
    public void testCreateConnectorWithDynamicConnection()
    {
        BigQueryPlugin plugin = new BigQueryPlugin();
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        assertThat(factory).isInstanceOf(BigQueryConnectorFactory.class);

        factory.create(
                        "test",
                        Map.of(
                                "bootstrap.quiet", "true",
                                "bigquery.authentication.type", "DYNAMIC_CONNECTION",
                                "bigquery.project-id.credential-name", "project_id",
                                "bigquery.parent-project-id.credential-name", "parent_project_id",
                                "bigquery.credentials-key.credential-name", "credentials_key",
                                "bigquery.view-materialization-project.credential-name", "view_materialization_project",
                                "bigquery.view-materialization-dataset.credential-name", "view_materialization_dataset"),
                        new TestingConnectorContext())
                .shutdown();
    }
}
