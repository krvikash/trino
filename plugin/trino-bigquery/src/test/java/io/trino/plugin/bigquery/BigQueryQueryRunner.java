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
package io.trino.plugin.bigquery;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableResult;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.airlift.log.Level;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.airlift.units.Duration;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.SqlExecutor;
import io.trino.tpch.TpchTable;
import org.intellij.lang.annotations.Language;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.cloud.bigquery.BigQuery.DatasetDeleteOption.deleteContents;
import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.QueryAssertions.copyTpchTables;
import static io.trino.testing.TestingProperties.requiredNonEmptySystemProperty;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;

public final class BigQueryQueryRunner
{
    static final String BIGQUERY_CREDENTIALS_KEY = requiredNonEmptySystemProperty("testing.bigquery.credentials-key");
    static final String DUMMY_BIGQUERY_CREDENTIALS_KEY = "ewogICAgInR5cGUiOiAic2VydmljZV9hY2NvdW50IiwKICAgICJwcm9qZWN0X2lkIjogInByZXN0b3Rlc3QiLAogICAgInByaXZhdGVfa2V5X2lkIjogIngiLAogICAgInByaXZhdGVfa2V5IjogIi0tLS0tQkVHSU4gUFJJVkFURSBLRVktLS0tLVxuTUlJRXZRSUJBREFOQmdrcWhraUc5dzBCQVFFRkFBU0NCS2N3Z2dTakFnRUFBb0lCQVFDM3E0NkcrdlRtdllmR1xuUEVCcUZST2MwWllEUFE4Z1VRYlEvaWRiYXBQc0s3TUxIZEx1RUdzQkF4SjMyYkdKV2FXV1pKMlZvKzA2Y3E4UlxuM1VZdVJ5RDBvNVk5OTV3d0t5YUVMdjVHTHFxMlpkQ0U2MGNqbE8yeXM4dUg4MVJMQStOME9zUzI0WXAvTmw1V1xuMHl4bkVuaW5VSW5Hb1VteVJ0K1V1aTIxNTQ3WTZFeDFKMGdqVndoNWtBNTJBcG0xVzVjZ3JKUWgwMlZTNUZERlxudEtnWjlKNFUyZVQvM2RNUkFlN0dLaWtseGpMTktjSzJ6T3JuYVpzb0pBTnNrZ0xMdjhPaDJEdlpQbWd1dmtxL1xuUU4yRVRCSXVLRUxCbEN4ZHZkRVp5L2pPcUVmdW1xcWI4VTVqVk4vdit6Q0pZVHREcVRGVWxLRVZDbEVNV3BCUFxuR3dIUzg5MlRBZ01CQUFFQ2dnRUFVOWxuTE9vZXFjUTIydUlneWcwck1mbGdrY1ByUnVhV3hReHlMVUsvbXg3c1xuRXhRZmVuMVdURlQ1dG10VXFJNmJrTWdJUlF0Y1BzV2lkUFplbHJ2MEtKc1IrT0kwbEt6dVhZUVNvem1reDdZOVxuZHFEdWppanNSeHZidkFuekhuZjgrOC9raEZUODVFeU96dmFERzk4TDQ5NVp0NnRrT0pZd2RmWjA3Y2x6cGtQSVxudVNDMGRTMldFckZnT0JiK3BJZGFwU3dSN1gxOTlROGNsenhjYlVUUUJJaGJDMnFhUmUvelFBdHNIS3ArMHRRVFxuWEI0Z3A1bitXc0pGM2lmTlYwdkZ0VWRRUlNCNFBmRzExRW1lczBQTFpxV1ZYb2xGdWpVQW8xS0o3dXFWTVpoUlxuQTF6VEpEbzNaaklHUllvbmRHQWRHR1hrMU1rd1JCcGoxR0FRb08xSm9RS0JnUURjUFhLcjF0MlBVbnJMelN1UFxuNVM2ek9WMUVzNkpJbmpVaGtXNFFhcTQ4RFRYNEg2bkdTYkdyYW5tbVpyQXlWNytEeXZPWGVzaC9ITzJROWtlaFxuRlczaUVtQzBCZE9FeWVuRUJUNThidHR5VzlMVUtBVjhjendYTno4Q3lSQ0xGd292UDIzUjFkS3BZdGtsR0l6NVxuWjJaMEF1SEtzcWd2TC9Jeng2bU1QNU0rQXdLQmdRRFZmZ2ttMUJPVzhad3JvNFFjbE43bnlKN01lQ3BCTFZycVxuUU9OcThqeE5EaGpsT0VDSElZZmFTUUZLYXkwa2pBTndTQjRMYURuTXJTbmRJYWxqV3F1LzBtdThMLzNwQzg4MlxuOUhpNU1Mc1Jjb0trNDY5UnRLRVIxWEwvcE5sb1NTd2dkZWJtUWk3clhNUzFCQks4aFZ0UFFObmV0RE1sLy9JTFxuS3YzbmtycFZNUUtCZ0dnaUdiVU1PK2dIUEk1ZUxRbTFlRFkvbWt6Z2pvdTlXaXZNQW5sNnAzVTNYZHc2eEdBL1xuK2VTdHpHVVVTcDBUQmpkL1gxdXhMMW1DeVFUd25YK1pqVUlHSkhrYUJCL1dCRlN0a2hUdHFZN1J3Y2FVUWJ2TlxuRkkxNWpxNTNlUDM2MzlMbEw3eTJXQXZFOUJ6cEZjYmEwQU5zVld3c3V2N01zYjB2MjRlM2k1d1hBb0dCQUswWlxuL2kyZmN5ckdTRXdSendLbHFuN2c2ZkQ3MWJiM0lXb2lwc0tHR21LWDlaT1ZvcXh1Z1lwNSt6UHQ1ckpsWER4a1xuSFFnK3YrNjIwT1RkY0V5QXJoVmdkYjRtWTRmYjdXMnZsMXNBcWcwaGZkQllWRVM1WW9mbE85TVFSTDhMNVYyRVxuZTIxamFFdXA4a3liT3Qza2V2NnRwSG13UG5Dbk1BZmlHZkR6eFdWaEFvR0FYa1k5bjNsSDFISDJBUDhzMkNnNVxuN3o3NVhLYWtxWE9CMkNhTWFuOWxJd0FCVzhSam1IKzRiU2VVQ0kwM1hRRExrY1R3T0N3QStrL3FvZldBeW1ldVxudzU4Vzh5cGlWVGpDVDErUzh5VjhYL0htTERVa3VsTnUvY2psYlJPdnJmSlRIL2pNbVhhTEQxeVZlYXlxOFlGZFxubnl6SmpiR1BwdGsvYVRTYk5rQmpvdWM9XG4tLS0tLUVORCBQUklWQVRFIEtFWS0tLS0tXG4iLAogICAgImNsaWVudF9lbWFpbCI6ICJ4IiwKICAgICJjbGllbnRfaWQiOiAieCIsCiAgICAiYXV0aF91cmkiOiAiaHR0cHM6Ly9hY2NvdW50cy5nb29nbGUuY29tL28vb2F1dGgyL2F1dGgiLAogICAgInRva2VuX3VyaSI6ICJodHRwczovL29hdXRoMi5nb29nbGVhcGlzLmNvbS90b2tlbiIsCiAgICAiYXV0aF9wcm92aWRlcl94NTA5X2NlcnRfdXJsIjogImh0dHBzOi8vd3d3Lmdvb2dsZWFwaXMuY29tL29hdXRoMi92MS9jZXJ0cyIsCiAgICAiY2xpZW50X3g1MDlfY2VydF91cmwiOiAiaHR0cHM6Ly93d3cuZ29vZ2xlYXBpcy5jb20vcm9ib3QvdjEvbWV0YWRhdGEveDUwOS9wcmVzdG90ZXN0JTQwcHJlc3RvdGVzdC5pYW0uZ3NlcnZpY2VhY2NvdW50LmNvbSIKfQo=";
    public static final String TPCH_SCHEMA = "tpch";
    public static final String TEST_SCHEMA = "test";

    static {
        Logging logging = Logging.initialize();
        logging.setLevel("com.google.cloud.bigquery.storage", Level.OFF);
    }

    private BigQueryQueryRunner() {}

    public static Builder builder()
    {
        return new Builder(BIGQUERY_CREDENTIALS_KEY);
    }

    public static Builder builder(String credentialsKey)
    {
        return new Builder(credentialsKey);
    }

    public static final class Builder
            extends DistributedQueryRunner.Builder<Builder>
    {
        private String credentialsKey;
        private Map<String, String> connectorProperties = ImmutableMap.of();
        private List<TpchTable<?>> initialTables = ImmutableList.of();

        private Builder(String credentialsKey)
        {
            super(testSessionBuilder()
                    .setCatalog("bigquery")
                    .setSchema(TPCH_SCHEMA)
                    .build());
            this.credentialsKey = requireNonNull(credentialsKey, "credentialsKey is null");
        }

        @CanIgnoreReturnValue
        public Builder setConnectorProperties(Map<String, String> connectorProperties)
        {
            this.connectorProperties = connectorProperties;
            return this;
        }

        @CanIgnoreReturnValue
        public Builder setInitialTables(Iterable<TpchTable<?>> initialTables)
        {
            this.initialTables = ImmutableList.copyOf(requireNonNull(initialTables, "initialTables is null"));
            return this;
        }

        @Override
        public DistributedQueryRunner build()
                throws Exception
        {
            DistributedQueryRunner queryRunner = super.build();
            try {
                queryRunner.installPlugin(new TpchPlugin());
                queryRunner.createCatalog("tpch", "tpch");

                // note: additional copy via ImmutableList so that if fails on nulls
                Map<String, String> connectorProperties = new HashMap<>(ImmutableMap.copyOf(this.connectorProperties));
                connectorProperties.putIfAbsent("bigquery.credentials-key", credentialsKey);
                connectorProperties.putIfAbsent("bigquery.views-enabled", "true");
                connectorProperties.putIfAbsent("bigquery.view-expire-duration", "30m");
                connectorProperties.putIfAbsent("bigquery.rpc-retries", "10");
                connectorProperties.putIfAbsent("bigquery.rpc-retry-delay", "200ms");
                connectorProperties.putIfAbsent("bigquery.rpc-retry-delay-multiplier", "1.5");
                connectorProperties.putIfAbsent("bigquery.rpc-timeout", "30s");

                queryRunner.installPlugin(new BigQueryPlugin());
                queryRunner.createCatalog("bigquery", "bigquery", connectorProperties);

                queryRunner.execute("CREATE SCHEMA IF NOT EXISTS " + TPCH_SCHEMA);
                queryRunner.execute("CREATE SCHEMA IF NOT EXISTS " + TEST_SCHEMA);
                copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, initialTables);
                return queryRunner;
            }
            catch (Throwable e) {
                closeAllSuppress(e, queryRunner);
                throw e;
            }
        }
    }

    public static class BigQuerySqlExecutor
            implements SqlExecutor
    {
        private static final Map.Entry<String, String> BIG_QUERY_SQL_EXECUTOR_LABEL = Map.entry("ci-automation-source", "trino_tests_big_query_sql_executor");

        private final BigQuery bigQuery;

        public BigQuerySqlExecutor()
        {
            this(BIGQUERY_CREDENTIALS_KEY);
        }

        public BigQuerySqlExecutor(String credentialsKey)
        {
            this.bigQuery = createBigQueryClient(credentialsKey);
        }

        @Override
        public void execute(@Language("SQL") String sql)
        {
            executeQuery(sql);
        }

        public TableResult executeQuery(@Language("SQL") String sql)
        {
            try {
                return bigQuery.query(QueryJobConfiguration.of(sql));
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }

        public void createDataset(String datasetName)
        {
            createDataset(DatasetId.of(datasetName));
        }

        public void createDataset(DatasetId datasetId)
        {
            DatasetInfo dataset = DatasetInfo.newBuilder(datasetId)
                    .setLabels(ImmutableMap.copyOf(ImmutableSet.of(BIG_QUERY_SQL_EXECUTOR_LABEL)))
                    .build();
            bigQuery.create(dataset);
        }

        public void dropDatasetIfExists(String dataset)
        {
            dropDatasetIfExists(DatasetId.of(dataset));
        }

        public void dropDatasetIfExists(DatasetId dataset)
        {
            bigQuery.delete(dataset, deleteContents());
        }

        public List<String> getTableNames(String dataset)
        {
            ImmutableList.Builder<String> tableNames = ImmutableList.builder();
            for (Table table : bigQuery.listTables(DatasetId.of(dataset)).iterateAll()) {
                tableNames.add(table.getTableId().getTable());
            }
            return tableNames.build();
        }

        public BigQuery getBigQuery()
        {
            return bigQuery;
        }

        private static BigQuery createBigQueryClient(String credentialsKey)
        {
            try {
                InputStream jsonKey = new ByteArrayInputStream(Base64.getDecoder().decode(credentialsKey));
                return BigQueryOptions.newBuilder()
                        .setCredentials(ServiceAccountCredentials.fromStream(jsonKey))
                        .setRetrySettings(new RetryOptionsConfigurer(new BigQueryRpcConfig()
                                .setRetries(10)
                                .setRetryDelay(Duration.valueOf("200ms"))
                                .setRetryMultiplier(1.5)
                                .setTimeout(Duration.valueOf("30s")))
                                .retrySettings())
                        .build()
                        .getService();
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    static void main()
            throws Exception
    {
        QueryRunner queryRunner = BigQueryQueryRunner.builder()
                .setCoordinatorProperties(Map.of("http-server.http.port", "8080"))
                .setInitialTables(TpchTable.getTables())
                .build();
        Logger log = Logger.get(BigQueryQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
