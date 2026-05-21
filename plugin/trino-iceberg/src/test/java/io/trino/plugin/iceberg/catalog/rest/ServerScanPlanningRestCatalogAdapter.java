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

import com.google.common.collect.ImmutableMap;
import io.airlift.http.server.HttpServerConfig;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.http.server.ServerFeature;
import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.node.NodeInfo;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.rest.HTTPRequest;
import org.apache.iceberg.rest.RESTCatalogAdapter;
import org.apache.iceberg.rest.RESTCatalogServlet;
import org.apache.iceberg.rest.RESTResponse;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.rest.responses.ErrorResponse;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.function.Consumer;

import static org.apache.iceberg.Files.localInput;
import static org.apache.iceberg.Files.localOutput;
import static org.apache.iceberg.rest.RESTCatalogProperties.SCAN_PLANNING_MODE;
import static org.apache.iceberg.rest.RESTCatalogProperties.ScanPlanningMode.SERVER;

/**
 * Wraps a backing {@link Catalog} and announces server-side scan planning by injecting
 * {@code scan-planning-mode=server} as a catalog-wide override in the {@link ConfigResponse}.
 */
public class ServerScanPlanningRestCatalogAdapter
        extends RESTCatalogAdapter
{
    public ServerScanPlanningRestCatalogAdapter(Catalog delegate)
    {
        super(delegate);
    }

    @Override
    protected <T extends RESTResponse> T execute(
            HTTPRequest request,
            Class<T> responseType,
            Consumer<ErrorResponse> errorHandler,
            Consumer<Map<String, String>> responseHeaders)
    {
        T response = super.execute(request, responseType, errorHandler, responseHeaders);
        if (response instanceof ConfigResponse configResponse) {
            ConfigResponse modified = ConfigResponse.builder()
                    .withDefaults(configResponse.defaults())
                    .withOverrides(configResponse.overrides())
                    .withOverride(SCAN_PLANNING_MODE, SERVER.name())
                    .withEndpoints(configResponse.endpoints())
                    .withIdempotencyKeyLifetime(configResponse.idempotencyKeyLifetime())
                    .build();
            return responseType.cast(modified);
        }
        return response;
    }

    public static JdbcCatalog buildBackendCatalog(Path warehousePath)
    {
        ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
        try {
            properties.put(CatalogProperties.URI, "jdbc:h2:file:" + Files.createTempFile(null, null).toAbsolutePath());
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        properties.put(JdbcCatalog.PROPERTY_PREFIX + "username", "user");
        properties.put(JdbcCatalog.PROPERTY_PREFIX + "password", "password");
        properties.put(JdbcCatalog.PROPERTY_PREFIX + "schema-version", "V1");
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, "file://" + warehousePath.toAbsolutePath());

        JdbcCatalog catalog = new JdbcCatalog(
                _ -> new FileIO()
                {
                    @Override
                    public InputFile newInputFile(String path)
                    {
                        return localInput(toFilesystemPath(path));
                    }

                    @Override
                    public OutputFile newOutputFile(String path)
                    {
                        return localOutput(toFilesystemPath(path));
                    }

                    @Override
                    public void deleteFile(String path)
                    {
                        try {
                            Files.deleteIfExists(Path.of(toFilesystemPath(path)));
                        }
                        catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    }
                },
                null,
                true);
        catalog.initialize("backend_jdbc", properties.buildOrThrow());
        return catalog;
    }

    private static String toFilesystemPath(String location)
    {
        if (location.startsWith("file:")) {
            return Path.of(URI.create(location)).toString();
        }
        return location;
    }

    public static TestingHttpServer startTestServer(ServerScanPlanningRestCatalogAdapter adapter)
            throws Exception
    {
        NodeInfo nodeInfo = new NodeInfo("test");
        HttpServerConfig config = new HttpServerConfig()
                .setHttpPort(0)
                .setHttpEnabled(true);
        HttpServerInfo httpServerInfo = new HttpServerInfo(config, nodeInfo);
        TestingHttpServer server = new TestingHttpServer(
                "rest-catalog",
                httpServerInfo,
                nodeInfo,
                config,
                new RESTCatalogServlet(adapter),
                ServerFeature.builder()
                        .withLegacyUriCompliance(true)
                        .build());
        server.start();
        return server;
    }
}
