/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.troubleshooting;

import com.starburstdata.presto.server.StarburstQueryRunner;
import io.starburst.server.troubleshooting.TroubleshootingTestHelper.Unzipped;
import io.trino.Session;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.security.Identity;
import io.trino.testing.DistributedQueryRunner;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static com.starburstdata.presto.server.StarburstClientCapabilities.QUERY_TROUBLESHOOTING;
import static io.starburst.server.troubleshooting.TroubleshootingTestHelper.getTroubleshootingDataForQuery;
import static io.trino.SystemSessionProperties.QUERY_MAX_MEMORY_PER_NODE;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

public class TestLimitJfrWorkerCollected
{
    private static final String AUTHORIZED_USER = "bob";

    private static final Session troubleshootedSession = testSessionBuilder()
            .setClientCapabilities(Set.of(QUERY_TROUBLESHOOTING.name()))
            .setSystemProperty(QUERY_MAX_MEMORY_PER_NODE, "10MB")
            .setIdentity(Identity.ofUser(AUTHORIZED_USER))
            .setCatalog("tpch")
            .build();

    @TempDir
    private Path tmpDir;

    private static DistributedQueryRunner createQueryRunner(int maxCollectedWorkers)
            throws Exception
    {
        DistributedQueryRunner queryRunner = StarburstQueryRunner.builder(troubleshootedSession)
                .setCoordinatorProperties(Map.of(
                        "insights.authorized-users", AUTHORIZED_USER,
                        "troubleshooting.jfr.max-collected-workers", String.valueOf(maxCollectedWorkers)))
                .build();

        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");

        return queryRunner;
    }

    @ParameterizedTest
    @MethodSource("workersCollected")
    public void testLimitedWorkersCollected(int maxCollectedWorkers, int expectedWorkersCollected)
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = createQueryRunner(maxCollectedWorkers)) {
            Unzipped inputsMap = getTroubleshootingDataForQuery(queryRunner, troubleshootedSession, "SHOW CATALOGS", tmpDir);

            assertThat(inputsMap.contents().keySet()).areExactly(expectedWorkersCollected, new Condition<>(key -> key.contains("recordings/worker-"), "worker recording"));
        }
    }

    public static Stream<Arguments> workersCollected()
    {
        return Stream.of(
                Arguments.of(0, 0),
                Arguments.of(1, 1),
                Arguments.of(2, 2),
                Arguments.of(Integer.MAX_VALUE, 2));
    }
}
