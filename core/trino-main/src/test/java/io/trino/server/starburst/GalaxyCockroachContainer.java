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
package io.trino.server.starburst;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.starburst.stargate.database.ExtendedCockroachContainer;
import org.testcontainers.containers.CockroachContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.startupcheck.OneShotStartupCheckStrategy;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static io.trino.server.starburst.GalaxyImageConstants.STARGATE_DOCKER_REPO;
import static io.trino.server.starburst.GalaxyImageConstants.STARGATE_IMAGE_TAG;
import static java.time.Duration.ofMinutes;

public class GalaxyCockroachContainer
        implements Closeable
{
    private static final Logger log = Logger.get(GalaxyCockroachContainer.class);

    private static final String MIGRATION_IMAGE = STARGATE_DOCKER_REPO + "migration-tool:" + STARGATE_IMAGE_TAG;

    private CockroachContainer cockroach;
    private Network network;

    public GalaxyCockroachContainer()
    {
        try {
            log.info("Starting Cockroach");
            Stopwatch cockroachStart = Stopwatch.createStarted();
            network = Network.newNetwork();
            cockroach = new ExtendedCockroachContainer();
            cockroach.setNetwork(network);
            cockroach.setNetworkAliases(ImmutableList.of("cockroach"));
            cockroach.start();
            log.info("Cockroach started in %.1f s".formatted(cockroachStart.elapsed(TimeUnit.MILLISECONDS) / 1000.));

            log.info("Running Database Migration");
            Stopwatch databaseMigration = Stopwatch.createStarted();
            try (GenericContainer<?> migration = new GenericContainer<>(MIGRATION_IMAGE)) {
                migration.setNetwork(network);
                migration.addEnv("DB_PASSWORD", cockroach.getPassword());
                migration.setCommand(
                        "--debug",
                        "--verbose",
                        "--url",
                        "jdbc:postgresql://cockroach:26257/postgres",
                        "--user",
                        cockroach.getUsername());
                migration.withStartupCheckStrategy(new OneShotStartupCheckStrategy().withTimeout(ofMinutes(4)));
                migration.start();
                migration.stop();
            }
            log.info("Database migrated in %.1f s".formatted(databaseMigration.elapsed(TimeUnit.MILLISECONDS) / 1000.));
        }
        catch (Throwable t) {
            log.error(t, "Failed to start Galaxy Cockroach container");
            try {
                close();
            }
            catch (IOException e) {
                log.error(e, "Failed to clean up cockroach docker resources");
            }
            throw t;
        }
    }

    public Network getNetwork()
    {
        return network;
    }

    public String getUsername()
    {
        return cockroach.getUsername();
    }

    public String getPassword()
    {
        return cockroach.getPassword();
    }

    public String getJdbcUrl()
    {
        return cockroach.getJdbcUrl();
    }

    @Override
    public void close()
            throws IOException
    {
        if (cockroach != null) {
            cockroach.close();
            cockroach = null;
        }
        if (network != null) {
            network.close();
            network = null;
        }
    }
}
