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

import io.airlift.http.client.HttpClientConfig;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.starburst.materialization.metastore.MetastoreId;
import io.starburst.materialization.metastore.RawMaterializationMetastore;
import io.starburst.materialization.metastore.client.HttpMaterializationMetastore;
import io.starburst.materialization.metastore.client.MaterializationMetastoreClientConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestMaterializationMetastoreRestRoundTrip
        extends AbstractRawMaterializationMetastoreTest
{
    private JettyHttpClient httpClient;

    @BeforeAll
    public void createHttpClient()
    {
        httpClient = new JettyHttpClient(new HttpClientConfig());
    }

    @AfterAll
    public void closeHttpClient()
    {
        if (httpClient != null) {
            httpClient.close();
        }
    }

    @Override
    protected JdbcDatabaseContainer<?> createDbContainer()
    {
        return new PostgreSQLContainer("postgres:16");
    }

    @Override
    protected RawMaterializationMetastore singleTenant(MetastoreId metastoreId)
    {
        MaterializationMetastoreClientConfig config = new MaterializationMetastoreClientConfig()
                .setBaseUri(server().baseUri())
                .setMetastoreId(metastoreId.id());
        return new HttpMaterializationMetastore(config, httpClient, _ -> {});
    }
}
