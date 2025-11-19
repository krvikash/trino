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

package io.trino.plugin.openapi;

import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.airlift.log.Level;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.plugin.memory.MemoryPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Verify.verify;
import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.testing.TestingSession.testSessionBuilder;

public final class OpenApiQueryRunner
{
    static {
        Logging logger = Logging.initialize();
        logger.setLevel("io.trino.plugin.openapi", Level.DEBUG);
        logger.setLevel("io.trino", Level.INFO);
        logger.setLevel("io.airlift", Level.INFO);
    }

    private OpenApiQueryRunner() {}

    public static Builder builder(Map<String, Map<String, String>> openAPICatalogs)
    {
        if (openAPICatalogs.isEmpty()) {
            throw new IllegalArgumentException("openAPICatalogs is empty, required at least one catalog for a default.");
        }
        String initialCatalogName = openAPICatalogs.keySet().iterator().next();
        Builder builder = new Builder(initialCatalogName);
        openAPICatalogs.forEach(builder::addOpenAPICatalog);
        return builder;
    }

    public static final class Builder
            extends DistributedQueryRunner.Builder<Builder>
    {
        private final Map<String, Map<String, String>> openAPICatalogs = new HashMap<>();

        private Builder(String initialCatalogName)
        {
            super(testSessionBuilder()
                    .setCatalog(initialCatalogName)
                    .setSchema("default")
                    .build());
        }

        @CanIgnoreReturnValue
        public Builder addOpenAPICatalog(String catalogName, Map<String, String> catalogProperties)
        {
            verify(
                    catalogProperties.containsKey("openapi.spec-location") &&
                            catalogProperties.containsKey("openapi.base-uri"),
                    "catalogProperties must include spec-location and base-uri");
            openAPICatalogs.put(catalogName, catalogProperties);
            return this;
        }

        @Override
        public DistributedQueryRunner build()
                throws Exception
        {
            DistributedQueryRunner queryRunner = super.build();
            try {
                queryRunner.installPlugin(new MemoryPlugin());
                queryRunner.createCatalog("memory", "memory");

                queryRunner.installPlugin(new OpenApiPlugin());
                openAPICatalogs.forEach((name, properties) ->
                        queryRunner.createCatalog(name, "openapi", properties));

                return queryRunner;
            }
            catch (Throwable e) {
                closeAllSuppress(e, queryRunner);
                throw e;
            }
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        FastApiServer fastApiServer = new FastApiServer();
        ImmutableMap<String, String> openapiProperties = ImmutableMap.of(
                "openapi.http-client.log.enabled", "true",
                "openapi.spec-location", fastApiServer.getSpecUrl(),
                "openapi.base-uri", fastApiServer.getApiUrl());
        QueryRunner queryRunner = builder(Map.of("openapi", openapiProperties))
                .addCoordinatorProperty("http-server.http.port", "8080")
                .build();
        Logger log = Logger.get(OpenApiQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
