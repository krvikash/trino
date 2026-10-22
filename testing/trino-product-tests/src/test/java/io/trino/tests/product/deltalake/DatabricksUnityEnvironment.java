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
package io.trino.tests.product.deltalake;

import io.trino.testing.containers.TrinoProductTestContainer;
import io.trino.testing.containers.environment.ProductTestEnvironment;
import io.trino.testing.containers.environment.QueryResult;
import org.testcontainers.containers.Network;
import org.testcontainers.trino.TrinoContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.testing.SystemEnvironmentUtils.requireEnv;
import static java.util.Objects.requireNonNull;

/**
 * Base JUnit product-test environment for Trino on a Databricks Unity catalog.
 * Subclasses provide the storage configuration and can override the other hooks, for example to add a proxy in front of the Unity API.
 */
public abstract class DatabricksUnityEnvironment
        extends ProductTestEnvironment
{
    @FunctionalInterface
    private interface ConnectionFactory
    {
        Connection create()
                throws SQLException;
    }

    static {
        try {
            Class.forName("com.databricks.client.jdbc.Driver");
        }
        catch (ClassNotFoundException e) {
            throw new RuntimeException("Failed to load Databricks JDBC driver", e);
        }
    }

    private Network network;
    private TrinoContainer trino;

    @Override
    public void start()
    {
        if (trino != null && trino.isRunning()) {
            return;
        }

        network = Network.newNetwork();
        startDependencies(network);

        TrinoProductTestContainer.Builder builder = TrinoProductTestContainer.builder()
                .withNetwork(network);
        catalogs().forEach(builder::withCatalog);
        trino = builder.build();

        configureTrinoContainer(trino);
        TrinoProductTestContainer.startAndWait(trino);
    }

    protected void startDependencies(Network network) {}

    protected Map<String, Map<String, String>> catalogs()
    {
        return Map.of(
                "hive", hiveCatalogProperties(),
                "delta", deltaCatalogProperties(),
                "tpch", Map.of("connector.name", "tpch"));
    }

    protected Map<String, String> hiveCatalogProperties()
    {
        Map<String, String> properties = new HashMap<>();
        properties.put("connector.name", "hive");
        properties.putAll(unityMetastoreProperties());
        properties.put("hive.non-managed-table-writes-enabled", "true");
        properties.put("hive.security", "allow-all");
        properties.put("hive.delta-lake-catalog-name", "delta");
        properties.putAll(fileSystemProperties());
        return properties;
    }

    protected Map<String, String> deltaCatalogProperties()
    {
        Map<String, String> properties = new HashMap<>();
        properties.put("connector.name", "delta_lake");
        properties.putAll(unityMetastoreProperties());
        properties.put("delta.security", "allow-all");
        properties.putAll(fileSystemProperties());
        properties.put("delta.enable-non-concurrent-writes", "true");
        return properties;
    }

    protected Map<String, String> unityMetastoreProperties()
    {
        return Map.of(
                "hive.metastore", "unity",
                "hive.metastore.unity.host", "${ENV:DATABRICKS_HOST}",
                "hive.metastore.unity.token", "${ENV:DATABRICKS_TOKEN}",
                "hive.metastore.unity.catalog-name", "${ENV:DATABRICKS_UNITY_CATALOG_NAME}",
                "hive.metastore.unity.catalog-managed-table-enabled", "true",
                // Tests create and drop tables directly in Databricks, so Trino must not serve stale metadata
                "hive.metastore-cache-ttl", "0ms");
    }

    protected abstract Map<String, String> fileSystemProperties();

    protected void configureTrinoContainer(TrinoContainer trino)
    {
        configureStorageCredentials(trino);
        trino.withEnv("DATABRICKS_HOST", databricksHost());
        trino.withEnv("DATABRICKS_TOKEN", databricksToken());
        trino.withEnv("DATABRICKS_UNITY_CATALOG_NAME", unityCatalogName());
    }

    protected abstract void configureStorageCredentials(TrinoContainer trino);

    public abstract String unityCatalogName();

    public abstract String unityExternalLocation();

    protected abstract String databricksHost();

    protected abstract String databricksJdbcUrl();

    protected abstract String databricksLoginEnvironmentVariable();

    protected abstract String databricksTokenEnvironmentVariable();

    private String databricksLogin()
    {
        return Optional.ofNullable(System.getenv(databricksLoginEnvironmentVariable()))
                .filter(login -> !login.isBlank())
                .orElse("token");
    }

    private String databricksToken()
    {
        return requireEnv(databricksTokenEnvironmentVariable());
    }

    public QueryResult executeTrinoSql(String sql)
    {
        return executeSql(this::createTrinoConnection, sql, "Trino");
    }

    public QueryResult executeDatabricksSql(String sql)
    {
        return executeSql(this::createDatabricksConnection, sql, "Databricks");
    }

    @Override
    public Connection createTrinoConnection()
            throws SQLException
    {
        return TrinoProductTestContainer.createConnection(trino);
    }

    @Override
    public Connection createTrinoConnection(String user)
            throws SQLException
    {
        return TrinoProductTestContainer.createConnection(trino, user);
    }

    public Connection createDatabricksConnection()
            throws SQLException
    {
        return DriverManager.getConnection(
                appendJdbcOption(databricksJdbcUrl(), "SocketTimeout=120"),
                databricksLogin(),
                databricksToken());
    }

    @Override
    public String getTrinoJdbcUrl()
    {
        return trino != null ? trino.getJdbcUrl() : null;
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
        if (network != null) {
            network.close();
            network = null;
        }
    }

    private static QueryResult executeSql(ConnectionFactory connectionFactory, String sql, String engine)
    {
        try (Connection connection = connectionFactory.create();
                Statement statement = connection.createStatement()) {
            boolean hasResultSet = statement.execute(sql);
            if (hasResultSet) {
                try (ResultSet resultSet = statement.getResultSet()) {
                    return QueryResult.forResultSet(resultSet);
                }
            }

            int updateCount = statement.getUpdateCount();
            if (updateCount < 0) {
                updateCount = 0;
            }
            return QueryResult.forRows(List.of(List.of(updateCount)));
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to execute " + engine + " query: " + sql + " (" + e.getMessage() + ")", e);
        }
    }

    private static String appendJdbcOption(String jdbcUrl, String option)
    {
        requireNonNull(jdbcUrl, "jdbcUrl is null");
        requireNonNull(option, "option is null");

        int separator = option.indexOf('=');
        if (separator < 0) {
            throw new IllegalArgumentException("JDBC option must be key=value: " + option);
        }

        String key = option.substring(0, separator) + "=";
        if (jdbcUrl.contains(key)) {
            return jdbcUrl;
        }

        if (jdbcUrl.endsWith(";")) {
            return jdbcUrl + option;
        }
        return jdbcUrl + ";" + option;
    }
}
