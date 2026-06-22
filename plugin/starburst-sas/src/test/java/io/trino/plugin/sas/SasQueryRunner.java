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
package io.trino.plugin.sas;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.tpch.TpchTable;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;

public final class SasQueryRunner
{
    public static final String CATALOG = "sas";
    public static final String TPCH_SCHEMA = "tpch";

    private SasQueryRunner() {}

    public static Builder builder(Path dataDirectory)
    {
        return new Builder(dataDirectory);
    }

    public static final class Builder
            extends DistributedQueryRunner.Builder<Builder>
    {
        private final Path dataDirectory;
        private List<TpchTable<?>> initialTables = ImmutableList.of();

        private Builder(Path dataDirectory)
        {
            super(testSessionBuilder()
                    .setCatalog(CATALOG)
                    .setSchema(TPCH_SCHEMA)
                    .build());
            this.dataDirectory = requireNonNull(dataDirectory, "dataDirectory is null");
        }

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
                if (!initialTables.isEmpty()) {
                    queryRunner.installPlugin(new TpchPlugin());
                    queryRunner.createCatalog(TPCH_SCHEMA, "tpch", ImmutableMap.of());
                }

                queryRunner.installPlugin(new SasPlugin());
                queryRunner.createCatalog(
                        CATALOG,
                        "sas",
                        ImmutableMap.of("sas.data-directory", dataDirectory.toUri().toString()));

                if (!initialTables.isEmpty()) {
                    Path schemaDir = dataDirectory.resolve(TPCH_SCHEMA);
                    Files.createDirectories(schemaDir);
                    for (TpchTable<?> table : initialTables) {
                        loadTpchTable(queryRunner, schemaDir, table);
                    }
                }

                return queryRunner;
            }
            catch (Throwable e) {
                closeAllSuppress(e, queryRunner);
                throw e;
            }
        }

        private static void loadTpchTable(DistributedQueryRunner queryRunner, Path schemaDir, TpchTable<?> table)
        {
            try (SasTpchLoader loader = new SasTpchLoader(queryRunner.getCoordinator(), schemaDir, table.getTableName())) {
                loader.execute("SELECT * FROM tpch.tiny." + table.getTableName());
            }
        }
    }

    static void main()
            throws Exception
    {
        Path dataDirectory = Path.of("plugin/starburst-sas/src/test/resources").toAbsolutePath();
        @SuppressWarnings("resource")
        DistributedQueryRunner queryRunner = builder(dataDirectory)
                .addCoordinatorProperty("http-server.http.port", "8080")
                .build();
        Logger log = Logger.get(SasQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
