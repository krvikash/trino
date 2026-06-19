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
package io.trino.server.starburst.security;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import com.google.inject.Module;
import io.airlift.security.pem.PemReader;
import io.jsonwebtoken.JwtParser;
import io.trino.server.security.Authenticator;
import io.trino.server.security.PortalAuthenticator;
import io.trino.server.security.TestResourceSecurity;
import io.trino.server.testing.TestingTrinoServer;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.io.File;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.Date;
import java.util.Optional;

import static com.google.common.hash.Hashing.sha256;
import static com.google.inject.multibindings.MapBinder.newMapBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static io.trino.client.OkHttpUtil.setupSsl;
import static io.trino.server.security.jwt.JwtUtil.newJwtBuilder;
import static io.trino.server.security.jwt.JwtUtil.newJwtParserBuilder;
import static jakarta.servlet.http.HttpServletResponse.SC_OK;
import static jakarta.servlet.http.HttpServletResponse.SC_UNAUTHORIZED;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.Instant.now;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestGalaxyTrinoAuthenticatorIntegration
{
    private static final String LOCALHOST_KEYSTORE = Resources.getResource("cert/localhost.pem").getPath();
    private static final MediaType PLAIN_UTF8 = MediaType.get("text/plain; charset=utf-8");
    private static final String GALAXY_HEADER = "X-Galaxy-Authentication";
    private static final String TEST_ISSUER = "test-galaxy-issuer";
    private static final String TEST_ACCOUNT_ID = "a-11111111";
    private static final String TEST_DEPLOYMENT_ID = "test-deployment";
    private static final String TEST_USERNAME = "test-user";
    private static final String TEST_USER_ID = "u-1111111111";
    private static final String TEST_ROLE_ID = "r-1111111111";

    private static final PrivateKey PRIVATE_KEY;
    private static final PublicKey PUBLIC_KEY;

    static {
        try {
            PRIVATE_KEY = PemReader.loadPrivateKey(new File(Resources.getResource("jwk/jwk-rsa-private.pem").toURI()), Optional.empty());
            PUBLIC_KEY = PemReader.loadPublicKey(new File(Resources.getResource("jwk/jwk-rsa-public.pem").getPath()));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @AutoClose
    private TestingTrinoServer server;

    private OkHttpClient client;

    @BeforeAll
    public void setup()
            throws Exception
    {
        server = TestingTrinoServer.builder()
                .setProperties(ImmutableMap.<String, String>builder()
                        .put("http-server.https.enabled", "true")
                        .put("http-server.https.keystore.path", LOCALHOST_KEYSTORE)
                        .put("http-server.https.keystore.key", "")
                        .put("http-server.authentication.type", "galaxy")
                        .put("web-ui.enabled", "false")
                        .buildOrThrow())
                .setAdditionalModule(testModule())
                .build();

        OkHttpClient.Builder clientBuilder = new OkHttpClient.Builder()
                .followRedirects(false);
        setupSsl(clientBuilder,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                false,
                Optional.of(LOCALHOST_KEYSTORE),
                Optional.empty(),
                Optional.empty(),
                false);
        client = clientBuilder.build();
    }

    @Test
    public void testValidTokenAuthenticatesUser()
            throws Exception
    {
        try (Response response = client.newCall(
                galaxyRequest(httpsUrl("/protocol/identity"), validToken())
                        .build()).execute()) {
            assertThat(response.code()).isEqualTo(SC_OK);
            assertThat(response.header("user")).isEqualTo(TEST_USERNAME);
        }
    }

    @Test
    public void testMissingTokenRejected()
            throws Exception
    {
        try (Response response = client.newCall(
                new Request.Builder()
                        .url(httpsUrl("/protocol/identity"))
                        .build()).execute()) {
            assertThat(response.code()).isEqualTo(SC_UNAUTHORIZED);
        }
    }

    @Test
    public void testInvalidTokenRejected()
            throws Exception
    {
        try (Response response = client.newCall(
                galaxyRequest(httpsUrl("/protocol/identity"), "not.a.valid.jwt")
                        .build()).execute()) {
            assertThat(response.code()).isEqualTo(SC_UNAUTHORIZED);
        }
    }

    @Test
    public void testExpiredTokenRejected()
            throws Exception
    {
        String expiredToken = newJwtBuilder()
                .signWith(PRIVATE_KEY)
                .issuer(TEST_ISSUER)
                .subject(TEST_DEPLOYMENT_ID)
                .audience().add(TEST_ACCOUNT_ID).and()
                .expiration(Date.from(now().minusSeconds(60)))
                .claim("username", TEST_USERNAME)
                .claim("user_id", TEST_USER_ID)
                .claim("role_id", TEST_ROLE_ID)
                .compact();

        try (Response response = client.newCall(
                galaxyRequest(httpsUrl("/protocol/identity"), expiredToken)
                        .build()).execute()) {
            assertThat(response.code()).isEqualTo(SC_UNAUTHORIZED);
        }
    }

    @Test
    public void testWrongIssuerRejected()
            throws Exception
    {
        String wrongIssuerToken = newJwtBuilder()
                .signWith(PRIVATE_KEY)
                .issuer("wrong-issuer")
                .subject(TEST_DEPLOYMENT_ID)
                .audience().add(TEST_ACCOUNT_ID).and()
                .expiration(Date.from(now().plusSeconds(60)))
                .claim("username", TEST_USERNAME)
                .claim("user_id", TEST_USER_ID)
                .claim("role_id", TEST_ROLE_ID)
                .compact();

        try (Response response = client.newCall(
                galaxyRequest(httpsUrl("/protocol/identity"), wrongIssuerToken)
                        .build()).execute()) {
            assertThat(response.code()).isEqualTo(SC_UNAUTHORIZED);
        }
    }

    @Test
    public void testWrongDeploymentIdRejected()
            throws Exception
    {
        String wrongDeploymentToken = newJwtBuilder()
                .signWith(PRIVATE_KEY)
                .issuer(TEST_ISSUER)
                .subject("wrong-deployment-id")
                .audience().add(TEST_ACCOUNT_ID).and()
                .expiration(Date.from(now().plusSeconds(60)))
                .claim("username", TEST_USERNAME)
                .claim("user_id", TEST_USER_ID)
                .claim("role_id", TEST_ROLE_ID)
                .compact();

        try (Response response = client.newCall(
                galaxyRequest(httpsUrl("/protocol/identity"), wrongDeploymentToken)
                        .build()).execute()) {
            assertThat(response.code()).isEqualTo(SC_UNAUTHORIZED);
        }
    }

    @Test
    public void testWrongAudienceRejected()
            throws Exception
    {
        String wrongAudienceToken = newJwtBuilder()
                .signWith(PRIVATE_KEY)
                .issuer(TEST_ISSUER)
                .subject(TEST_DEPLOYMENT_ID)
                .audience().add("a-99999999").and()
                .expiration(Date.from(now().plusSeconds(60)))
                .claim("username", TEST_USERNAME)
                .claim("user_id", TEST_USER_ID)
                .claim("role_id", TEST_ROLE_ID)
                .compact();

        try (Response response = client.newCall(
                galaxyRequest(httpsUrl("/protocol/identity"), wrongAudienceToken)
                        .build()).execute()) {
            assertThat(response.code()).isEqualTo(SC_UNAUTHORIZED);
        }
    }

    @Test
    public void testPostStatementRejectedAsDeprecated()
            throws Exception
    {
        Request request = new Request.Builder()
                .url(httpsUrl("/v1/statement"))
                .header(GALAXY_HEADER, validToken())
                .post(RequestBody.create("SELECT 1", PLAIN_UTF8))
                .build();

        try (Response response = client.newCall(request).execute()) {
            assertThat(response.code()).isEqualTo(SC_UNAUTHORIZED);
        }
    }

    @Test
    public void testPortalIdentityWithValidToken()
            throws Exception
    {
        try (Response response = client.newCall(
                galaxyRequest(httpsUrl("/portal/identity"), validToken())
                        .build()).execute()) {
            assertThat(response.code()).isEqualTo(SC_OK);
            assertThat(response.header("user")).isEqualTo(TEST_USERNAME);
        }
    }

    @Test
    public void testStatementBodyHashMismatchRejected()
            throws Exception
    {
        String queryBody = "SELECT 1";
        String wrongHash = sha256().hashString("SELECT 2", UTF_8).toString();

        String token = newJwtBuilder()
                .signWith(PRIVATE_KEY)
                .issuer(TEST_ISSUER)
                .subject(TEST_DEPLOYMENT_ID)
                .audience().add(TEST_ACCOUNT_ID).and()
                .expiration(Date.from(now().plusSeconds(60)))
                .claim("username", TEST_USERNAME)
                .claim("user_id", TEST_USER_ID)
                .claim("role_id", TEST_ROLE_ID)
                .claim("request_expiry", Date.from(now().plusSeconds(60)))
                .claim("statement_hash", wrongHash)
                .compact();

        try (Response response = client.newCall(statementRequest("hashtest", "slug1", queryBody, token)).execute()) {
            assertThat(response.code()).isEqualTo(SC_UNAUTHORIZED);
        }
    }

    @Test
    public void testStatementBodyHashExpiredRejected()
            throws Exception
    {
        String queryBody = "SELECT 1";
        String hash = sha256().hashString(queryBody, UTF_8).toString();

        String token = newJwtBuilder()
                .signWith(PRIVATE_KEY)
                .issuer(TEST_ISSUER)
                .subject(TEST_DEPLOYMENT_ID)
                .audience().add(TEST_ACCOUNT_ID).and()
                .expiration(Date.from(now().plusSeconds(60)))
                .claim("username", TEST_USERNAME)
                .claim("user_id", TEST_USER_ID)
                .claim("role_id", TEST_ROLE_ID)
                .claim("request_expiry", Date.from(now().minusSeconds(60)))
                .claim("statement_hash", hash)
                .compact();

        try (Response response = client.newCall(statementRequest("hashtest2", "slug2", queryBody, token)).execute()) {
            assertThat(response.code()).isEqualTo(SC_UNAUTHORIZED);
        }
    }

    @Test
    public void testStatementBodyHashSucceeds()
            throws Exception
    {
        String queryBody = "SELECT 1";
        String hash = sha256().hashString(queryBody, UTF_8).toString();

        String token = newJwtBuilder()
                .signWith(PRIVATE_KEY)
                .issuer(TEST_ISSUER)
                .subject(TEST_DEPLOYMENT_ID)
                .audience().add(TEST_ACCOUNT_ID).and()
                .expiration(Date.from(now().plusSeconds(60)))
                .claim("username", TEST_USERNAME)
                .claim("user_id", TEST_USER_ID)
                .claim("role_id", TEST_ROLE_ID)
                .claim("request_expiry", Date.from(now().plusSeconds(60)))
                .claim("statement_hash", hash)
                .compact();

        try (Response response = client.newCall(statementRequest("hashtest3", "slug3", queryBody, token)).execute()) {
            assertThat(response.code()).isEqualTo(SC_OK);
        }
    }

    private Request.Builder galaxyRequest(String url, String token)
    {
        return new Request.Builder()
                .url(url)
                .header(GALAXY_HEADER, token);
    }

    private String httpsUrl(String path)
    {
        return server.getHttpsBaseUrl().resolve(path).toString();
    }

    private Request statementRequest(String queryId, String slug, String body, String token)
    {
        return new Request.Builder()
                .url(server.getHttpsBaseUrl().resolve("/v1/statement/queued/%s/%s".formatted(queryId, slug)).toString())
                .header(GALAXY_HEADER, token)
                .put(RequestBody.create(body, PLAIN_UTF8))
                .build();
    }

    private String validToken()
    {
        return newJwtBuilder()
                .signWith(PRIVATE_KEY)
                .issuer(TEST_ISSUER)
                .subject(TEST_DEPLOYMENT_ID)
                .audience().add(TEST_ACCOUNT_ID).and()
                .expiration(Date.from(now().plusSeconds(60)))
                .claim("username", TEST_USERNAME)
                .claim("user_id", TEST_USER_ID)
                .claim("role_id", TEST_ROLE_ID)
                .compact();
    }

    private static Module testModule()
    {
        JwtParser parser = newJwtParserBuilder()
                .setSigningKey(PUBLIC_KEY)
                .requireSubject(TEST_DEPLOYMENT_ID)
                .build();
        GalaxyTrinoAuthenticator authenticator = new GalaxyTrinoAuthenticator(
                new GalaxyAuthenticatorController(TEST_ISSUER, TEST_ACCOUNT_ID, token -> parser.parseClaimsJws(token).getBody()));
        return binder -> {
            jaxrsBinder(binder).bind(TestResourceSecurity.TestResource.class);
            newMapBinder(binder, String.class, Authenticator.class)
                    .addBinding("galaxy")
                    .toInstance(authenticator);
            newOptionalBinder(binder, PortalAuthenticator.class)
                    .setBinding()
                    .toInstance(authenticator);
        };
    }
}
