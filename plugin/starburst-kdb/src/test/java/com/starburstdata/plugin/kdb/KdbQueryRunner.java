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
package com.starburstdata.plugin.kdb;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.TestingSession;
import io.trino.tpch.TpchTable;

import java.util.List;

import static com.starburstdata.plugin.kdb.KdbTpchLoader.loadTpchTables;

public final class KdbQueryRunner
{
    private KdbQueryRunner() {}

    public static Builder builder(KdbContainer server)
    {
        return new Builder(server)
                .addConnectorProperty("kdb.host", server.host())
                .addConnectorProperty("kdb.port", String.valueOf(server.port()));
    }

    public static final class Builder
    {
        private final KdbContainer server;
        private final ImmutableMap.Builder<String, String> connectorProperties = ImmutableMap.builder();
        private final ImmutableMap.Builder<String, String> coordinatorProperties = ImmutableMap.builder();
        private List<TpchTable<?>> initialTables = ImmutableList.of();

        private Builder(KdbContainer server)
        {
            this.server = server;
        }

        public Builder addConnectorProperty(String key, String value)
        {
            connectorProperties.put(key, value);
            return this;
        }

        public Builder addCoordinatorProperty(String key, String value)
        {
            coordinatorProperties.put(key, value);
            return this;
        }

        @CanIgnoreReturnValue
        public Builder setInitialTables(Iterable<TpchTable<?>> initialTables)
        {
            this.initialTables = ImmutableList.copyOf(initialTables);
            return this;
        }

        public DistributedQueryRunner build()
                throws Exception
        {
            Session session = TestingSession.testSessionBuilder()
                    .setCatalog("kdb")
                    .setSchema("default")
                    .build();

            DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session)
                    .setWorkerCount(3)
                    .setCoordinatorProperties(coordinatorProperties.buildOrThrow())
                    .build();

            try {
                queryRunner.installPlugin(new TpchPlugin());
                queryRunner.createCatalog("tpch", "tpch", ImmutableMap.of());

                queryRunner.installPlugin(new KdbPlugin());
                queryRunner.createCatalog("kdb", "starburst_kdb", connectorProperties.buildOrThrow());

                KdbClient client = server.client();
                loadTpchTables(client, initialTables);
                return queryRunner;
            }
            catch (Throwable e) {
                queryRunner.close();
                throw e;
            }
        }
    }

    static void main()
            throws Exception
    {
        KdbContainer server = new KdbContainer();
        @SuppressWarnings("resource")
        DistributedQueryRunner queryRunner = builder(server)
                .addCoordinatorProperty("http-server.http.port", "8080")
                .setInitialTables(List.of(TpchTable.NATION, TpchTable.REGION, TpchTable.CUSTOMER, TpchTable.ORDERS))
                .build();

        Logger log = Logger.get(KdbQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
