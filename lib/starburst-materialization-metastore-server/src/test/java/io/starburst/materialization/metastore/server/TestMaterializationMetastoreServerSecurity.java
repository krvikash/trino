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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.bootstrap.AutoCloseableCloser;
import io.airlift.http.client.HeaderName;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.http.client.Request;
import io.airlift.http.client.StatusResponseHandler.StatusResponse;
import io.airlift.http.client.jetty.JettyHttpClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.net.URI;

import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static io.starburst.materialization.metastore.server.RequiresSecurityDynamicFeature.VALID_TOKEN;
import static jakarta.ws.rs.core.HttpHeaders.AUTHORIZATION;
import static jakarta.ws.rs.core.Response.Status.OK;
import static jakarta.ws.rs.core.Response.Status.UNAUTHORIZED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestMaterializationMetastoreServerSecurity
{
    private final AutoCloseableCloser closer = AutoCloseableCloser.create();
    private JettyHttpClient httpClient;
    private URI materializationsUri;

    @BeforeAll
    void setUp()
    {
        PostgreSQLContainer container = closer.register(new PostgreSQLContainer("postgres:16"));
        container.start();
        TestingMaterializationMetastoreServer server = closer.register(new TestingMaterializationMetastoreServer(
                container.getJdbcUrl(),
                container.getUsername(),
                container.getPassword(),
                ImmutableMap.of(),
                SecuredMaterializationMetastoreResource.class,
                ImmutableList.of(binder -> jaxrsBinder(binder).bind(RequiresSecurityDynamicFeature.class))));
        httpClient = closer.register(new JettyHttpClient(new HttpClientConfig()));
        materializationsUri = uriBuilderFrom(server.baseUri())
                .replacePath("/materializations/v1/ms-test")
                .build();
    }

    @AfterAll
    void tearDown()
            throws Exception
    {
        closer.close();
    }

    @Test
    void testRequestWithoutTokenIsRejected()
    {
        assertThat(statusCode(prepareGet().setUri(materializationsUri).build()))
                .isEqualTo(UNAUTHORIZED.getStatusCode());
    }

    @Test
    void testRequestWithInvalidTokenIsRejected()
    {
        Request request = prepareGet()
                .setUri(materializationsUri)
                .addHeader(HeaderName.of(AUTHORIZATION), "Bearer wrong-token")
                .build();
        assertThat(statusCode(request)).isEqualTo(UNAUTHORIZED.getStatusCode());
    }

    @Test
    void testRequestWithValidTokenIsAccepted()
    {
        Request request = prepareGet()
                .setUri(materializationsUri)
                .addHeader(HeaderName.of(AUTHORIZATION), "Bearer " + VALID_TOKEN)
                .build();
        assertThat(statusCode(request)).isEqualTo(OK.getStatusCode());
    }

    private int statusCode(Request request)
    {
        StatusResponse response = httpClient.execute(request, createStatusResponseHandler());
        return response.getStatusCode();
    }
}
