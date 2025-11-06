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

import com.google.inject.Binder;
import com.google.inject.Key;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.starburst.server.troubleshooting.TroubleshootingTestHelper.Unzipped;
import io.trino.Session;
import io.trino.node.InternalNodeManager;
import io.trino.spi.security.Identity;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tests.tpch.TpchQueryRunner;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.starburst.server.troubleshooting.DistributedTroubleshootingTestHelper.getTroubleshootingDataForQuery;
import static io.starburst.server.troubleshooting.TroubleshootingSessionProperties.TROUBLESHOOTING_JFR_MAX_COLLECTED_WORKERS;
import static io.starburst.server.troubleshooting.TroubleshootingSessionProperties.TROUBLESHOOTING_TRACE_MAX_COLLECTED_WORKERS;
import static io.trino.client.AdditionalClientCapabilities.QUERY_TROUBLESHOOTING;
import static io.trino.node.NodeState.ACTIVE;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.Math.min;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

public class TestLimitedWorkersCollected
        extends AbstractTestQueryFramework
{
    private static final String AUTHORIZED_USER = "bob";

    private static final Session troubleshootedSession = testSessionBuilder()
            .setClientCapabilities(Set.of(QUERY_TROUBLESHOOTING.name()))
            .setIdentity(Identity.ofUser(AUTHORIZED_USER))
            .setCatalog("tpch")
            .build();
    private static final int WORKER_COUNT = 3;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createQueryRunner(0);
    }

    private static DistributedQueryRunner createQueryRunner(int maxCollectedWorkers)
            throws Exception
    {
        DistributedQueryRunner queryRunner = TpchQueryRunner.builder()
                .setCoordinatorProperties(Map.of(
                        "troubleshooting.jfr.max-collected-workers", String.valueOf(maxCollectedWorkers),
                        "troubleshooting.trace.max-collected-workers", String.valueOf(maxCollectedWorkers)))
                .setAdditionalModule(new AbstractConfigurationAwareModule()
                {
                    @Override
                    protected void setup(Binder binder)
                    {
                        binder.bind(TroubleshootingAccessControl.class)
                                .toInstance(identity -> AUTHORIZED_USER.equals(identity.getUser()));
                        install(new TroubleshootingModule());
                    }
                })
                .setWorkerCount(WORKER_COUNT)
                .build();
        // wait for all the workers to announce itself in the discovery service
        InternalNodeManager nodeManager = queryRunner.getCoordinator().getInstance(Key.get(InternalNodeManager.class));
        await().atMost(5, SECONDS)
                .untilAsserted(() -> assertThat(nodeManager.getNodes(ACTIVE)).hasSizeGreaterThanOrEqualTo(queryRunner.getNodeCount()));

        return queryRunner;
    }

    @ParameterizedTest
    @MethodSource("configWorkersCollected")
    public void testConfigLimitedWorkersCollected(int maxCollectedWorkers, int expectedWorkersCollected, @TempDir Path tmpDir)
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = createQueryRunner(maxCollectedWorkers)) {
            assertTroubleshootingDataCollected(queryRunner, troubleshootedSession, expectedWorkersCollected, expectedWorkersCollected, tmpDir);
        }
    }

    @ParameterizedTest
    @MethodSource("sessionWorkersCollected")
    public void testSessionLimitedWorkersCollected(int maxJfrCollectedWorkers, int maxTraceCollectedWorkers, @TempDir Path tmpDir)
            throws Exception
    {
        assertTroubleshootingDataCollected(
                getDistributedQueryRunner(),
                testSessionBuilder(troubleshootedSession)
                        .setSystemProperty(TROUBLESHOOTING_JFR_MAX_COLLECTED_WORKERS, String.valueOf(maxJfrCollectedWorkers))
                        .setSystemProperty(TROUBLESHOOTING_TRACE_MAX_COLLECTED_WORKERS, String.valueOf(maxTraceCollectedWorkers))
                        .build(),
                min(maxJfrCollectedWorkers, WORKER_COUNT),
                min(maxTraceCollectedWorkers, WORKER_COUNT), tmpDir);
    }

    private void assertTroubleshootingDataCollected(DistributedQueryRunner queryRunner, Session session, int expectedJfrWorkersCollected, int expectedTraceWorkersCollected, Path tmpDir)
            throws Exception
    {
        Unzipped inputsMap = getTroubleshootingDataForQuery(queryRunner, session, "SHOW CATALOGS", tmpDir);

        assertThat(inputsMap.contents().keySet()).areExactly(expectedJfrWorkersCollected, new Condition<>(key -> key.contains("recordings/worker-"), "jfr worker recording"));
        assertThat(inputsMap.contents().keySet()).areExactly(expectedTraceWorkersCollected, new Condition<>(key -> key.contains("traces/opentelemetry-worker-"), "traces worker recording"));
        // assert traces and jfr profiles are collected on the same workers or one is a subset of the other
        List<String> jfrWorkers = inputsMap.contents().keySet()
                .stream()
                .filter(key -> key.contains("recordings/worker-"))
                .map(key -> extractGroup(".*recordings/worker-(.*).jfr", key))
                .collect(toImmutableList());
        List<String> traceWorkers = inputsMap.contents().keySet()
                .stream()
                .filter(key -> key.contains("traces/opentelemetry-worker-"))
                .map(key -> extractGroup(".*traces/opentelemetry-worker-(.*).grpc.gz", key))
                .collect(toImmutableList());
        if (traceWorkers.size() >= jfrWorkers.size()) {
            assertThat(traceWorkers).containsAll(jfrWorkers);
        }
        else {
            assertThat(jfrWorkers).containsAll(traceWorkers);
        }
    }

    private static String extractGroup(String pattern, String value)
    {
        Matcher m = Pattern.compile(pattern).matcher(value);
        assertThat(m.find()).isTrue();
        return m.group(1);
    }

    public static Stream<Arguments> configWorkersCollected()
    {
        return Stream.of(
                Arguments.of(0, 0),
                Arguments.of(1, 1),
                Arguments.of(2, 2),
                Arguments.of(3, 3),
                Arguments.of(Integer.MAX_VALUE, 3));
    }

    public static Stream<Arguments> sessionWorkersCollected()
    {
        return Stream.of(
                Arguments.of(0, 1),
                Arguments.of(1, 0),
                Arguments.of(1, 1),
                Arguments.of(2, 1),
                Arguments.of(1, 3),
                Arguments.of(Integer.MAX_VALUE, 3));
    }
}
