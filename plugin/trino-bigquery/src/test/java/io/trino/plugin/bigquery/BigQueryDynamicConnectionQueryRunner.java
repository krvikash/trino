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

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Level;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;

import java.util.Map;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.testing.TestingSession.testSessionBuilder;

public final class BigQueryDynamicConnectionQueryRunner
{
    public static final String PROJECT_ID_CREDENTIAL_NAME = "project_id";
    public static final String PARENT_PROJECT_ID_CREDENTIAL_NAME = "parent_project_id";
    public static final String CREDENTIALS_KEY_CREDENTIAL_NAME = "credentials_key";
    public static final String VIEW_MATERIALIZATION_PROJECT_CREDENTIAL_NAME = "view_materialization_project";
    public static final String VIEW_MATERIALIZATION_DATASET_CREDENTIAL_NAME = "view_materialization_dataset";

    public static final String TPCH_SCHEMA = "tpch";

    static {
        Logging logging = Logging.initialize();
        logging.setLevel("com.google.cloud.bigquery.storage", Level.OFF);
    }

    private BigQueryDynamicConnectionQueryRunner() {}

    public static Builder builder()
    {
        return new Builder();
    }

    public static final class Builder
            extends DistributedQueryRunner.Builder<Builder>
    {
        private Builder()
        {
            super(testSessionBuilder()
                    .setCatalog("bigquery")
                    .setSchema(TPCH_SCHEMA)
                    .build());
        }

        @Override
        public DistributedQueryRunner build()
                throws Exception
        {
            DistributedQueryRunner queryRunner = super.build();
            try {
                queryRunner.installPlugin(new TpchPlugin());
                queryRunner.createCatalog("tpch", "tpch");

                Map<String, String> connectorProperties = ImmutableMap.<String, String>builder()
                        .put("bigquery.authentication.type", "DYNAMIC_CONNECTION")
                        .put("bigquery.project-id.credential-name", PROJECT_ID_CREDENTIAL_NAME)
                        .put("bigquery.parent-project-id.credential-name", PARENT_PROJECT_ID_CREDENTIAL_NAME)
                        .put("bigquery.credentials-key.credential-name", CREDENTIALS_KEY_CREDENTIAL_NAME)
                        .put("bigquery.view-materialization-project.credential-name", VIEW_MATERIALIZATION_PROJECT_CREDENTIAL_NAME)
                        .put("bigquery.view-materialization-dataset.credential-name", VIEW_MATERIALIZATION_DATASET_CREDENTIAL_NAME)
                        .put("bigquery.views-enabled", "true")
                        .put("bigquery.view-expire-duration", "30m")
                        .put("bigquery.rpc-retries", "10")
                        .put("bigquery.rpc-retry-delay", "200ms")
                        .put("bigquery.rpc-retry-delay-multiplier", "1.5")
                        .put("bigquery.rpc-timeout", "30s")
                        .buildOrThrow();

                queryRunner.installPlugin(new BigQueryPlugin());
                queryRunner.createCatalog("bigquery", "bigquery", connectorProperties);

                return queryRunner;
            }
            catch (Throwable e) {
                closeAllSuppress(e, queryRunner);
                throw e;
            }
        }
    }

    static void main()
            throws Exception
    {
        QueryRunner queryRunner = BigQueryDynamicConnectionQueryRunner.builder()
                .setCoordinatorProperties(Map.of("http-server.http.port", "8080"))
                .build();
        Logger log = Logger.get(BigQueryDynamicConnectionQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
