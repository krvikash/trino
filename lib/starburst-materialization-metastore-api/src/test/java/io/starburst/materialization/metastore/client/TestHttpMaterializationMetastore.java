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
package io.starburst.materialization.metastore.client;

import com.google.common.net.MediaType;
import io.airlift.http.client.HeaderName;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.http.client.testing.TestingHttpClient;
import io.starburst.stargate.id.AccountId;
import io.trino.spi.connector.CatalogSchemaTableName;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.concurrent.atomic.AtomicReference;

import static io.airlift.http.client.testing.TestingResponse.mockResponse;
import static io.starburst.materialization.metastore.client.HttpMaterializationMetastore.ACCOUNT_ID_HEADER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestHttpMaterializationMetastore
{
    private static final RequestAuthenticator NO_AUTH = _ -> {};

    private static MaterializationMetastoreClientConfig config()
    {
        return new MaterializationMetastoreClientConfig()
                .setBaseUri(URI.create("http://metastore:8080"))
                .setMetastoreId("ms-1")
                .setAccountId(new AccountId("a-12345678"));
    }

    private static Request captureRemoveRequest(RequestAuthenticator requestAuthenticator)
    {
        AtomicReference<Request> captured = new AtomicReference<>();
        TestingHttpClient httpClient = new TestingHttpClient(request -> {
            captured.set(request);
            return mockResponse(HttpStatus.OK, MediaType.JSON_UTF_8, "");
        });
        HttpMaterializationMetastore metastore = new HttpMaterializationMetastore(config(), httpClient, requestAuthenticator);

        metastore.remove(new CatalogSchemaTableName("cat", "sch", "mv"));
        return captured.get();
    }

    @Test
    public void testRequestTargetsMetastoreScopedPath()
    {
        Request request = captureRemoveRequest(NO_AUTH);
        assertThat(request.getUri().getPath()).isEqualTo("/materializations/v1/ms-1/cat/sch/mv");
        assertThat(request.getMethod()).isEqualTo("DELETE");
        // with a no-op authenticator the client adds no auth header of its own
        assertThat(request.getHeaders().get(HeaderName.of("X-Custom-Auth"))).isEmpty();
    }

    @Test
    public void testRequestAuthenticatorIsAppliedToEveryRequest()
    {
        Request request = captureRemoveRequest(builder -> builder.addHeader("X-Custom-Auth", "token-123"));
        assertThat(request.getHeaders().get(HeaderName.of("X-Custom-Auth")))
                .containsExactly("token-123");
    }

    @Test
    public void testAccountIdIsSentAsHeader()
    {
        Request request = captureRemoveRequest(NO_AUTH);
        assertThat(request.getHeaders().get(ACCOUNT_ID_HEADER)).containsExactly("a-12345678");
    }

    @Test
    public void testAccountIdIsRequired()
    {
        assertThatThrownBy(() -> new HttpMaterializationMetastore(config().setAccountId(null), new TestingHttpClient(_ -> mockResponse(HttpStatus.OK, MediaType.JSON_UTF_8, "")), NO_AUTH))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("accountId is null");
    }
}
