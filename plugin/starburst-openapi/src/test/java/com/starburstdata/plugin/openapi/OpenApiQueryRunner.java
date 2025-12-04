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
package com.starburstdata.plugin.openapi;

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
        logger.setLevel("com.starburstdata.plugin.openapi", Level.DEBUG);
        logger.setLevel("io.trino", Level.INFO);
        logger.setLevel("io.airlift", Level.INFO);
    }

    private OpenApiQueryRunner() {}

    public static Builder builder()
    {
        return new Builder();
    }

    public static final class Builder
            extends DistributedQueryRunner.Builder<Builder>
    {
        private final Map<String, String> connectorProperties = new HashMap<>();

        private Builder()
        {
            super(testSessionBuilder()
                    .setCatalog("openapi")
                    .setSchema("default")
                    .build());
        }

        @CanIgnoreReturnValue
        public Builder addConnectorProperties(Map<String, String> connectorProperties)
        {
            this.connectorProperties.putAll(connectorProperties);
            return this;
        }

        @Override
        public DistributedQueryRunner build()
                throws Exception
        {
            DistributedQueryRunner queryRunner = super.build();
            verify(
                    connectorProperties.containsKey("openapi.spec-location") &&
                            connectorProperties.containsKey("openapi.base-uri"),
                    "connectorProperties must include spec-location and base-uri");
            try {
                queryRunner.installPlugin(new MemoryPlugin());
                queryRunner.createCatalog("memory", "memory");

                queryRunner.installPlugin(new OpenApiPlugin());
                queryRunner.createCatalog("openapi", "openapi", connectorProperties);

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
        QueryRunner queryRunner = builder()
                .addConnectorProperties(ImmutableMap.<String, String>builder()
                        .put("openapi.http-client.log.enabled", "true")
                        .put("openapi.spec-location", fastApiServer.getSpecUrl())
                        .put("openapi.base-uri", fastApiServer.getApiUrl())
                        .buildOrThrow())
                .addCoordinatorProperty("http-server.http.port", "8080")
                .build();
        Logger log = Logger.get(OpenApiQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
