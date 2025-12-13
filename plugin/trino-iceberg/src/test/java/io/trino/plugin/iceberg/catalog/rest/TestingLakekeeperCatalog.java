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
package io.trino.plugin.iceberg.catalog.rest;

import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.StatusResponseHandler;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.json.JsonCodec;
import io.trino.testing.containers.Minio;
import org.intellij.lang.annotations.Language;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;

import java.io.Closeable;
import java.net.URI;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static io.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static io.trino.testing.containers.Minio.DEFAULT_HOST_NAME;
import static io.trino.testing.containers.Minio.MINIO_ACCESS_KEY;
import static io.trino.testing.containers.Minio.MINIO_API_PORT;
import static io.trino.testing.containers.Minio.MINIO_REGION;
import static io.trino.testing.containers.Minio.MINIO_SECRET_KEY;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.containers.PostgreSQLContainer.POSTGRESQL_PORT;

public final class TestingLakekeeperCatalog
        implements Closeable
{
    public static final String BUCKET = "test-bucket";
    public static final String WAREHOUSE = "lakekeeper";
    private static final String PROJECT_ID = "00000000-0000-0000-0000-000000000000";
    private static final String DB_URL_WRITE_KEY = "LAKEKEEPER__PG_DATABASE_URL_WRITE";
    private static final String DB_URL_READ_KEY = "LAKEKEEPER__PG_DATABASE_URL_READ";
    private static final String ENCRYPTION_KEY = "LAKEKEEPER__PG_ENCRYPTION_KEY";
    private static final String ENCRYPTION_VALUE = "This-is-NOT-Secure!";
    private static final String LAKEKEEPER_CATALOG_IMAGE = "quay.io/lakekeeper/catalog:v0.8.5";
    private static final int LAKEKEEPER_PORT = 8181;
    private static final HttpClient HTTP_CLIENT = new JettyHttpClient();

    private final GenericContainer<?> lakekeeperCatalog;
    private final GenericContainer<?> migrator;
    private final PostgreSQLContainer<?> postgresqlContainer;
    private final Minio minio;
    private final Network network;
    private final String prefix;

    public TestingLakekeeperCatalog()
    {
        network = Network.newNetwork();
        postgresqlContainer = startPostgreSql(network);
        String jdbcUrl = postgreSqlJdbcUrl(postgresqlContainer);

        migrator = new GenericContainer<>(LAKEKEEPER_CATALOG_IMAGE);
        migrator.withEnv(ENCRYPTION_KEY, ENCRYPTION_VALUE)
                .withEnv(DB_URL_READ_KEY, jdbcUrl)
                .withEnv(DB_URL_WRITE_KEY, jdbcUrl)
                .withNetwork(network)
                .withCommand("migrate")
                .start();

        minio = startMinio(network);
        minio.createBucket(BUCKET);

        lakekeeperCatalog = new GenericContainer<>(LAKEKEEPER_CATALOG_IMAGE);
        lakekeeperCatalog.withEnv(Map.of(
                        ENCRYPTION_KEY, ENCRYPTION_VALUE,
                        DB_URL_READ_KEY, jdbcUrl,
                        DB_URL_WRITE_KEY, jdbcUrl,
                        "LAKEKEEPER__QUEUE_CONFIG__POLL_INTERVAL", "\"1s\""))
                .withNetwork(network)
                .withExposedPorts(LAKEKEEPER_PORT)
                .withCommand("serve");

        lakekeeperCatalog.start();
        bootstrapServer();
        createCatalog();
        prefix = fetchPrefix();
    }

    @Override
    public void close()
    {
        migrator.close();
        lakekeeperCatalog.close();
        postgresqlContainer.close();
        minio.close();
        network.close();
    }

    public void dropWithoutPurge(String schema, String table)
    {
        Request request = Request.Builder.prepareDelete()
                .setUri(URI.create(restUri() + "/catalog/v1/" + prefix + "/namespaces/" + schema + "/tables/" + table + "?purgeRequested=false"))
                .setHeader(CONTENT_TYPE, "application/json")
                .build();
        StatusResponseHandler.StatusResponse statusResponse = HTTP_CLIENT.execute(request, createStatusResponseHandler());
        checkState(statusResponse.getStatusCode() == 204, "Failed to drop table. Status: %s", statusResponse.getStatusCode());
    }

    public String restUri()
    {
        return "http://%s:%s".formatted(lakekeeperCatalog.getHost(), lakekeeperCatalog.getMappedPort(LAKEKEEPER_PORT));
    }

    public String externalMinioAddress()
    {
        return minio.getMinioAddress();
    }

    private String fetchPrefix()
    {
        Request request = Request.Builder.prepareGet().setUri(URI.create(restUri() + "/catalog/v1/config?warehouse=" + WAREHOUSE)).build();
        Map<String, Object> resp = HTTP_CLIENT.execute(request, createJsonResponseHandler(JsonCodec.mapJsonCodec(String.class, Object.class)));
        @SuppressWarnings("unchecked")
        Map<String, String> overrides = ((Map<String, String>) resp.get("overrides"));
        return requireNonNull(overrides.get("prefix"));
    }

    private void bootstrapServer()
    {
        @Language("JSON")
        String body = "{\"accept-terms-of-use\": true}";
        Request request = Request.Builder.preparePost()
                .setUri(URI.create(restUri() + "/management/v1/bootstrap"))
                .setHeader(CONTENT_TYPE, "application/json")
                .setBodyGenerator(createStaticBodyGenerator(body, UTF_8))
                .build();
        HTTP_CLIENT.execute(request, createStatusResponseHandler());
    }

    private void createCatalog()
    {
        String body = JsonCodec.jsonCodec(Map.class).toJson(Map.of(
                "warehouse-name", WAREHOUSE,
                "project-id", PROJECT_ID,
                "storage-profile", Map.<String, Object>of(
                        "type", "s3",
                        "bucket", BUCKET,
                        "endpoint", "http://%s:%d".formatted(DEFAULT_HOST_NAME, MINIO_API_PORT),
                        "region", MINIO_REGION,
                        "path-style-access", true,
                        "flavor", "s3-compat",
                        "sts-enabled", true),
                "storage-credential", Map.<String, Object>of(
                        "type", "s3",
                        "credential-type", "access-key",
                        "aws-access-key-id", MINIO_ACCESS_KEY,
                        "aws-secret-access-key", MINIO_SECRET_KEY)));

        Request request = Request.Builder.preparePost()
                .setUri(URI.create(restUri() + "/management/v1/warehouse"))
                .setHeader(CONTENT_TYPE, "application/json")
                .setBodyGenerator(createStaticBodyGenerator(body, UTF_8))
                .build();

        StatusResponseHandler.StatusResponse response = HTTP_CLIENT.execute(
                request,
                createStatusResponseHandler());

        checkState(response.getStatusCode() == 201, "Failed to create catalog. Status: %s", response.getStatusCode());
    }

    private static Minio startMinio(Network network)
    {
        Minio minio = Minio.builder().withNetwork(network).build();
        minio.start();
        return minio;
    }

    private static PostgreSQLContainer<?> startPostgreSql(Network network)
    {
        PostgreSQLContainer<?> postgreSQLContainer = new PostgreSQLContainer<>("postgres:16");
        postgreSQLContainer.withNetwork(network).start();
        return postgreSQLContainer;
    }

    private static String postgreSqlJdbcUrl(PostgreSQLContainer<?> postgreSql)
    {
        return format("postgresql://%s:%s@%s:%d/%s",
                postgreSql.getUsername(),
                postgreSql.getPassword(),
                postgreSql.getContainerName().substring(1),
                POSTGRESQL_PORT,
                postgreSql.getDatabaseName());
    }
}
