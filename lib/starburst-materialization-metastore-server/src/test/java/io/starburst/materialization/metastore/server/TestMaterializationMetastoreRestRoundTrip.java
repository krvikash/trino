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

import io.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.http.client.Request;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.starburst.materialization.metastore.MetastoreId;
import io.starburst.materialization.metastore.RawMaterializationDefinition;
import io.starburst.materialization.metastore.RawMaterializationMetastore;
import io.starburst.materialization.metastore.client.HttpMaterializationMetastore;
import io.starburst.materialization.metastore.client.MaterializationMetastoreClientConfig;
import io.starburst.stargate.id.AccountId;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.util.List;

import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.json.JsonCodec.listJsonCodec;
import static io.starburst.materialization.metastore.server.TestingMaterializationMetastoreServer.TESTING_ACCOUNT_RESOLVER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
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
        return client(metastoreId, TESTING_ACCOUNT_RESOLVER.accountId(metastoreId));
    }

    @Test
    public void testRequestWithoutAccountIdIsAccepted()
    {
        RawMaterializationDefinition definition = definition("catalog", "schema", "mv_no_account");
        singleTenant(TEST_METASTORE_ID).createOrReplace(definition);

        // what clients predating account ids send
        Request request = prepareGet()
                .setUri(uriBuilderFrom(server().baseUri()).appendPath("materializations/v1").appendPath(TEST_METASTORE_ID.id()).build())
                .build();
        JsonResponse<List<RawMaterializationDefinition>> response = httpClient.execute(request, createFullJsonResponseHandler(listJsonCodec(RawMaterializationDefinition.class)));

        assertThat(response.getStatusCode()).isEqualTo(200);
        assertThat(response.getValue()).containsExactly(definition);
    }

    @Test
    public void testMismatchedAccountIdIsRejected()
    {
        RawMaterializationMetastore wrongAccount = client(TEST_METASTORE_ID, new AccountId("a-99999999"));

        assertThatThrownBy(wrongAccount::listMaterializations)
                .hasMessageContaining("Unexpected response: 403");
        assertThatThrownBy(() -> wrongAccount.createOrReplace(definition("catalog", "schema", "mv_wrong_account")))
                .hasMessageContaining("Unexpected response: 403");
        assertThat(singleTenant(TEST_METASTORE_ID).listMaterializations()).isEmpty();
    }

    private RawMaterializationMetastore client(MetastoreId metastoreId, AccountId accountId)
    {
        MaterializationMetastoreClientConfig config = new MaterializationMetastoreClientConfig()
                .setBaseUri(server().baseUri())
                .setMetastoreId(metastoreId.id())
                .setAccountId(accountId);
        return new HttpMaterializationMetastore(config, httpClient, _ -> {});
    }
}
