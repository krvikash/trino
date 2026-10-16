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
package io.trino.tests.product.iceberg;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.trino.testing.containers.Minio;
import io.trino.testing.containers.TrinoProductTestContainer;
import io.trino.testing.containers.environment.ProductTestEnvironment;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.startupcheck.OneShotStartupCheckStrategy;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.trino.TrinoContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.testing.containers.Minio.MINIO_API_PORT;
import static io.trino.testing.containers.Minio.MINIO_REGION;
import static io.trino.testing.containers.Minio.MINIO_ROOT_PASSWORD;
import static io.trino.testing.containers.Minio.MINIO_ROOT_USER;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.joining;

/**
 * Trino with an Iceberg REST catalog secured with {@code OAUTH2_PASSTHROUGH}.
 * <p>
 * Lakekeeper is the REST catalog and validates bearer tokens issued by Keycloak. Table data is stored in MinIO.
 * Clients obtain an access token from Keycloak and pass it to Trino in the extra credential where the coordinator
 * stores the OAuth2 access token of an authenticated user.
 */
public class IcebergRestOAuth2PassthroughEnvironment
        extends ProductTestEnvironment
{
    public static final String SCHEMA = "sep";
    public static final String USER = "alice";

    // Same as OAuth2TokenPassThrough.OAUTH2_ACCESS_TOKEN_PASSTHROUGH_CREDENTIAL
    private static final String OAUTH2_ACCESS_TOKEN_PASSTHROUGH_CREDENTIAL = "starburst.$internal.oauth2.access_token.passthrough";

    private static final String KEYCLOAK_IMAGE = "quay.io/keycloak/keycloak:26.0.8";
    private static final String KEYCLOAK_HOST = "keycloak";
    private static final int KEYCLOAK_PORT = 8080;
    private static final String KEYCLOAK_ADMIN = "admin";
    private static final String KEYCLOAK_ADMIN_PASSWORD = "admin";
    private static final String REALM = "sep-realm";
    private static final String CLIENT_ID = "sep-client";
    private static final String CLIENT_SECRET = "ATcy1mS0bHdnTpZ5653HQQUViogawCwC";
    private static final String USER_PASSWORD = "test";

    private static final String LAKEKEEPER_IMAGE = "quay.io/lakekeeper/catalog:v0.8.5";
    private static final String LAKEKEEPER_HOST = "lakekeeper";
    private static final int LAKEKEEPER_PORT = 8181;
    private static final String LAKEKEEPER_ENCRYPTION_KEY = "This-is-NOT-Secure!";
    private static final String WAREHOUSE = "lakekeeper";
    private static final String BUCKET = "test-bucket";

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(30))
            .build();

    private Network network;
    private GenericContainer<?> keycloak;
    private PostgreSQLContainer<?> postgresql;
    private GenericContainer<?> lakekeeperMigrate;
    private GenericContainer<?> lakekeeper;
    private Minio minio;
    private TrinoContainer trino;

    @Override
    public void start()
    {
        if (trino != null && trino.isRunning()) {
            return;
        }

        network = Network.newNetwork();

        // Keycloak tokens must carry the issuer that Lakekeeper resolves inside the network
        keycloak = new GenericContainer<>(DockerImageName.parse(KEYCLOAK_IMAGE))
                .withNetwork(network)
                .withNetworkAliases(KEYCLOAK_HOST)
                .withEnv("KEYCLOAK_ADMIN", KEYCLOAK_ADMIN)
                .withEnv("KEYCLOAK_ADMIN_PASSWORD", KEYCLOAK_ADMIN_PASSWORD)
                .withEnv("KC_HOSTNAME", "http://%s:%d".formatted(KEYCLOAK_HOST, KEYCLOAK_PORT))
                .withEnv("KC_HOSTNAME_BACKCHANNEL_DYNAMIC", "false")
                .withExposedPorts(KEYCLOAK_PORT)
                .withCommand("start-dev")
                .waitingFor(Wait.forHttp("/realms/master").forPort(KEYCLOAK_PORT).withStartupTimeout(Duration.ofMinutes(3)));
        keycloak.start();
        createRealm();

        minio = Minio.builder().withNetwork(network).build();
        minio.start();
        minio.createBucket(BUCKET);

        postgresql = new PostgreSQLContainer<>(DockerImageName.parse("postgres:16"))
                .withNetwork(network)
                .withNetworkAliases("postgresql");
        postgresql.start();
        String databaseUrl = "postgresql://%s:%s@postgresql:5432/%s".formatted(postgresql.getUsername(), postgresql.getPassword(), postgresql.getDatabaseName());

        lakekeeperMigrate = new GenericContainer<>(DockerImageName.parse(LAKEKEEPER_IMAGE))
                .withNetwork(network)
                .withEnv("LAKEKEEPER__PG_ENCRYPTION_KEY", LAKEKEEPER_ENCRYPTION_KEY)
                .withEnv("LAKEKEEPER__PG_DATABASE_URL_READ", databaseUrl)
                .withEnv("LAKEKEEPER__PG_DATABASE_URL_WRITE", databaseUrl)
                .withCommand("migrate")
                .withStartupCheckStrategy(new OneShotStartupCheckStrategy().withTimeout(Duration.ofMinutes(2)));
        lakekeeperMigrate.start();

        lakekeeper = new GenericContainer<>(DockerImageName.parse(LAKEKEEPER_IMAGE))
                .withNetwork(network)
                .withNetworkAliases(LAKEKEEPER_HOST)
                .withEnv("LAKEKEEPER__PG_ENCRYPTION_KEY", LAKEKEEPER_ENCRYPTION_KEY)
                .withEnv("LAKEKEEPER__PG_DATABASE_URL_READ", databaseUrl)
                .withEnv("LAKEKEEPER__PG_DATABASE_URL_WRITE", databaseUrl)
                .withEnv("LAKEKEEPER__OPENID_PROVIDER_URI", "http://%s:%d/realms/%s".formatted(KEYCLOAK_HOST, KEYCLOAK_PORT, REALM))
                .withEnv("LAKEKEEPER__OPENID_AUDIENCE", CLIENT_ID)
                .withEnv("LAKEKEEPER__OPENID_CLIENT_ID", CLIENT_ID)
                .withExposedPorts(LAKEKEEPER_PORT)
                .withCommand("serve")
                .waitingFor(Wait.forHttp("/health").forPort(LAKEKEEPER_PORT));
        lakekeeper.start();
        createWarehouse();

        trino = TrinoProductTestContainer.builder()
                .withNetwork(network)
                .withCatalog("iceberg", ImmutableMap.<String, String>builder()
                        .put("connector.name", "iceberg")
                        .put("iceberg.catalog.type", "rest")
                        .put("iceberg.rest-catalog.uri", "http://%s:%d/catalog".formatted(LAKEKEEPER_HOST, LAKEKEEPER_PORT))
                        .put("iceberg.rest-catalog.warehouse", WAREHOUSE)
                        .put("iceberg.rest-catalog.security", "OAUTH2_PASSTHROUGH")
                        .put("fs.s3.enabled", "true")
                        .put("s3.region", MINIO_REGION)
                        .put("s3.aws-access-key", MINIO_ROOT_USER)
                        .put("s3.aws-secret-key", MINIO_ROOT_PASSWORD)
                        .put("s3.endpoint", "http://%s:%d/".formatted(Minio.DEFAULT_HOST_NAME, MINIO_API_PORT))
                        .put("s3.path-style-access", "true")
                        .buildOrThrow())
                .build();
        TrinoProductTestContainer.startAndWait(trino);
    }

    /**
     * Obtains an access token for the given user from Keycloak.
     */
    public String fetchAccessToken(String user, String password)
    {
        return fetchToken(Map.of(
                "grant_type", "password",
                "client_id", CLIENT_ID,
                "client_secret", CLIENT_SECRET,
                "username", user,
                "password", password,
                "scope", "openid"));
    }

    public String fetchAccessToken()
    {
        return fetchAccessToken(USER, USER_PASSWORD);
    }

    /**
     * Opens a connection whose queries carry the given OAuth2 access token, like those of a user authenticated with OAuth2.
     */
    public Connection createTrinoConnectionWithAccessToken(String accessToken)
            throws SQLException
    {
        Properties properties = new Properties();
        properties.setProperty("user", USER);
        properties.setProperty("extraCredentials", OAUTH2_ACCESS_TOKEN_PASSTHROUGH_CREDENTIAL + ":" + accessToken);
        return DriverManager.getConnection(trino.getJdbcUrl(), properties);
    }

    @Override
    public Connection createTrinoConnection()
            throws SQLException
    {
        return TrinoProductTestContainer.createConnection(trino, USER);
    }

    @Override
    public Connection createTrinoConnection(String user)
            throws SQLException
    {
        return TrinoProductTestContainer.createConnection(trino, user);
    }

    @Override
    public String getTrinoJdbcUrl()
    {
        return trino.getJdbcUrl();
    }

    @Override
    public boolean isRunning()
    {
        return trino != null && trino.isRunning();
    }

    @Override
    protected void doClose()
    {
        if (trino != null) {
            trino.close();
            trino = null;
        }
        if (lakekeeper != null) {
            lakekeeper.close();
            lakekeeper = null;
        }
        if (lakekeeperMigrate != null) {
            lakekeeperMigrate.close();
            lakekeeperMigrate = null;
        }
        if (postgresql != null) {
            postgresql.close();
            postgresql = null;
        }
        if (minio != null) {
            minio.close();
            minio = null;
        }
        if (keycloak != null) {
            keycloak.close();
            keycloak = null;
        }
        if (network != null) {
            network.close();
            network = null;
        }
    }

    private void createRealm()
    {
        String adminToken = fetchToken("master", Map.of(
                "grant_type", "password",
                "client_id", "admin-cli",
                "username", KEYCLOAK_ADMIN,
                "password", KEYCLOAK_ADMIN_PASSWORD));

        Map<String, Object> audienceMapper = ImmutableMap.of(
                "name", "audience",
                "protocol", "openid-connect",
                "protocolMapper", "oidc-audience-mapper",
                "config", ImmutableMap.of(
                        "included.client.audience", CLIENT_ID,
                        "access.token.claim", "true"));
        Map<String, Object> realm = ImmutableMap.of(
                "realm", REALM,
                "enabled", true,
                "accessTokenLifespan", 3600,
                "clients", List.of(ImmutableMap.<String, Object>builder()
                        .put("clientId", CLIENT_ID)
                        .put("secret", CLIENT_SECRET)
                        .put("enabled", true)
                        .put("publicClient", false)
                        .put("directAccessGrantsEnabled", true)
                        .put("serviceAccountsEnabled", true)
                        .put("protocolMappers", List.of(audienceMapper))
                        .buildOrThrow()),
                "users", List.of(ImmutableMap.of(
                        "username", USER,
                        "enabled", true,
                        "email", USER + "@example.com",
                        "emailVerified", true,
                        "firstName", USER,
                        "lastName", USER,
                        "credentials", List.of(ImmutableMap.of(
                                "type", "password",
                                "value", USER_PASSWORD,
                                "temporary", false)))));

        send(HttpRequest.newBuilder(URI.create(keycloakUrl() + "/admin/realms"))
                .header("Authorization", "Bearer " + adminToken)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(toJson(realm))), 201);
    }

    private void createWarehouse()
    {
        String token = fetchToken(Map.of(
                "grant_type", "client_credentials",
                "client_id", CLIENT_ID,
                "client_secret", CLIENT_SECRET));
        String lakekeeperUrl = "http://%s:%d".formatted(lakekeeper.getHost(), lakekeeper.getMappedPort(LAKEKEEPER_PORT));

        send(HttpRequest.newBuilder(URI.create(lakekeeperUrl + "/management/v1/bootstrap"))
                .header("Authorization", "Bearer " + token)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(toJson(Map.of("accept-terms-of-use", true)))), 204);

        Map<String, Object> warehouse = ImmutableMap.of(
                "warehouse-name", WAREHOUSE,
                "storage-profile", ImmutableMap.<String, Object>builder()
                        .put("type", "s3")
                        .put("bucket", BUCKET)
                        .put("endpoint", "http://%s:%d".formatted(Minio.DEFAULT_HOST_NAME, MINIO_API_PORT))
                        .put("region", MINIO_REGION)
                        .put("path-style-access", true)
                        .put("flavor", "s3-compat")
                        .put("sts-enabled", true)
                        .buildOrThrow(),
                "storage-credential", ImmutableMap.of(
                        "type", "s3",
                        "credential-type", "access-key",
                        "aws-access-key-id", MINIO_ROOT_USER,
                        "aws-secret-access-key", MINIO_ROOT_PASSWORD));
        JsonNode response = send(HttpRequest.newBuilder(URI.create(lakekeeperUrl + "/management/v1/warehouse"))
                .header("Authorization", "Bearer " + token)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(toJson(warehouse))), 201);
        String warehouseId = response.get("warehouse-id").asText();

        send(HttpRequest.newBuilder(URI.create(lakekeeperUrl + "/catalog/v1/" + warehouseId + "/namespaces"))
                .header("Authorization", "Bearer " + token)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(toJson(Map.of("namespace", List.of(SCHEMA))))), 200);
    }

    private String fetchToken(Map<String, String> form)
    {
        return fetchToken(REALM, form);
    }

    private String fetchToken(String realm, Map<String, String> form)
    {
        String body = form.entrySet().stream()
                .map(entry -> URLEncoder.encode(entry.getKey(), UTF_8) + "=" + URLEncoder.encode(entry.getValue(), UTF_8))
                .collect(joining("&"));
        // Request the token with the issuer Lakekeeper resolves inside the network, not the mapped host port
        JsonNode response = send(HttpRequest.newBuilder(URI.create(keycloakUrl() + "/realms/" + realm + "/protocol/openid-connect/token"))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .POST(HttpRequest.BodyPublishers.ofString(body)), 200);
        return response.get("access_token").asText();
    }

    private String keycloakUrl()
    {
        return "http://%s:%d".formatted(keycloak.getHost(), keycloak.getMappedPort(KEYCLOAK_PORT));
    }

    private JsonNode send(HttpRequest.Builder request, int expectedStatus)
    {
        try {
            HttpResponse<String> response = httpClient.send(request.timeout(Duration.ofMinutes(1)).build(), HttpResponse.BodyHandlers.ofString());
            checkState(response.statusCode() == expectedStatus, "Unexpected status %s for %s: %s", response.statusCode(), response.uri(), response.body());
            if (response.body().isEmpty()) {
                return OBJECT_MAPPER.createObjectNode();
            }
            return OBJECT_MAPPER.readTree(response.body());
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private static String toJson(Object value)
    {
        try {
            return OBJECT_MAPPER.writeValueAsString(value);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
