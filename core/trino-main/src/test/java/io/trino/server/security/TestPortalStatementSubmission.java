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
package io.trino.server.security;

import com.google.inject.Scopes;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.spi.security.Identity;
import jakarta.ws.rs.container.ContainerRequestContext;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.util.Optional;

import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static jakarta.servlet.http.HttpServletResponse.SC_BAD_REQUEST;
import static jakarta.servlet.http.HttpServletResponse.SC_CONFLICT;
import static jakarta.servlet.http.HttpServletResponse.SC_NOT_FOUND;
import static jakarta.servlet.http.HttpServletResponse.SC_OK;
import static jakarta.servlet.http.HttpServletResponse.SC_UNAUTHORIZED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestPortalStatementSubmission
{
    private static final MediaType PLAIN_TEXT_UTF_8 = MediaType.get("text/plain; charset=utf-8");
    private static final String PORTAL_TOKEN_HEADER = "X-Portal-Token";
    private static final String TOKEN = "portal-user";

    @AutoClose
    private TestingTrinoServer server;
    private OkHttpClient client;

    @BeforeAll
    public void setup()
    {
        server = TestingTrinoServer.builder()
                .setAdditionalModule(binder ->
                        newOptionalBinder(binder, PortalAuthenticator.class)
                                .setBinding()
                                .to(TestingPortalAuthenticator.class).in(Scopes.SINGLETON))
                .build();
        client = new OkHttpClient();
    }

    @Test
    public void testAuthenticatedSubmissionSucceeds()
            throws IOException
    {
        assertResponseCode(putStatement("ok_query", "ok_slug", "SELECT 1", Optional.of(TOKEN)), SC_OK);
    }

    @Test
    public void testMissingTokenRejected()
            throws IOException
    {
        assertResponseCode(putStatement("unauthorized_query", "unauthorized_slug", "SELECT 1", Optional.empty()), SC_UNAUTHORIZED);
    }

    @Test
    public void testEmptyStatementRejected()
            throws IOException
    {
        assertResponseCode(putStatement("empty_query", "empty_slug", "", Optional.of(TOKEN)), SC_BAD_REQUEST);
    }

    @Test
    public void testMissingSlugStatementRejected()
            throws IOException
    {
        // without slug jax-rs does not match putStatement
        assertResponseCode(putStatement("missing_slug_query", "", "SELECT 1", Optional.of(TOKEN)), SC_NOT_FOUND);
    }

    @Test
    public void testRepeatedSubmissionIsIdempotent()
            throws IOException
    {
        assertResponseCode(putStatement("idempotent_query", "idempotent_slug", "SELECT 1", Optional.of(TOKEN)), SC_OK);
        assertResponseCode(putStatement("idempotent_query", "idempotent_slug", "SELECT 1", Optional.of(TOKEN)), SC_OK);
    }

    @Test
    public void testConflictingSlugRejected()
            throws IOException
    {
        assertResponseCode(putStatement("conflict_query", "first_slug", "SELECT 1", Optional.of(TOKEN)), SC_OK);
        assertResponseCode(putStatement("conflict_query", "second_slug", "SELECT 1", Optional.of(TOKEN)), SC_CONFLICT);
    }

    private Request putStatement(String queryId, String slug, String statement, Optional<String> token)
    {
        Request.Builder builder = new Request.Builder()
                .url(server.getBaseUrl().resolve("/v1/statement/queued/%s/%s".formatted(queryId, slug)).toString())
                .put(RequestBody.create(statement, PLAIN_TEXT_UTF_8));
        token.ifPresent(tokenHeader -> builder.header(PORTAL_TOKEN_HEADER, tokenHeader));
        return builder.build();
    }

    private void assertResponseCode(Request request, int expectedCode)
            throws IOException
    {
        try (Response response = client.newCall(request).execute()) {
            assertThat(response.code())
                    .describedAs(request.url().toString())
                    .isEqualTo(expectedCode);
        }
    }

    private static class TestingPortalAuthenticator
            implements PortalAuthenticator
    {
        @Override
        public Identity authenticate(ContainerRequestContext request)
                throws AuthenticationException
        {
            String token = request.getHeaderString(PORTAL_TOKEN_HEADER);
            if (isNullOrEmpty(token)) {
                throw new AuthenticationException("Missing portal token");
            }
            return Identity.ofUser(token);
        }
    }
}
