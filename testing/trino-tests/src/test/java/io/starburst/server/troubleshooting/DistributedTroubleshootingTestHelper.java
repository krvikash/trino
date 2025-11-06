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

import com.google.common.collect.ImmutableSet;
import com.google.inject.Key;
import io.starburst.server.troubleshooting.TroubleshootingTestHelper.Unzipped;
import io.trino.Session;
import io.trino.execution.QueryInfo;
import io.trino.execution.StageInfo;
import io.trino.execution.StagesInfo;
import io.trino.execution.TaskInfo;
import io.trino.execution.TaskStatus;
import io.trino.spi.QueryId;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.TestingTrinoClient;
import org.intellij.lang.annotations.Language;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.starburst.server.troubleshooting.TroubleshootingTestHelper.zipInputStreamToMap;

class DistributedTroubleshootingTestHelper
{
    private DistributedTroubleshootingTestHelper() {}

    public static Unzipped getTroubleshootingDataForQuery(DistributedQueryRunner queryRunner, Session session, @Language("SQL") String query, Path tmpDir)
            throws Exception
    {
        try (TestingTrinoClient client = new TestingTrinoClient(queryRunner.getCoordinator(), session)) {
            TroubleshootingContextManager troubleshootingContextManager = queryRunner.getCoordinator().getInstance(Key.get(TroubleshootingContextManager.class));
            InputStream inputStream = troubleshootingContextManager.getArchive(client.execute(query).getQueryId()).orElseThrow().get(10, TimeUnit.SECONDS);
            return zipInputStreamToMap(inputStream, tmpDir);
        }
    }

    public static List<Unzipped> findConfigZips(Unzipped inputsMap, String zipPrefix, Path tmpDir)
    {
        return inputsMap.contents().entrySet().stream()
                .filter(entry -> {
                    String path = entry.getKey();
                    return path.contains("/configs/" + zipPrefix) && path.endsWith(".zip");
                })
                .map(Map.Entry::getValue)
                .map(zipBytes -> zipInputStreamToMap(new ByteArrayInputStream(zipBytes), tmpDir))
                .toList();
    }

    public static Set<String> getNodesProcessingQuery(DistributedQueryRunner queryRunner, QueryId queryId)
    {
        try {
            QueryInfo queryInfo = queryRunner.getCoordinator().getQueryManager().getFullQueryInfo(queryId);
            return queryInfo.getStages()
                    .map(stages -> getNodeIdsProcessingQuery(queryRunner, stages))
                    .orElse(ImmutableSet.of());
        }
        catch (Exception e) {
            return Set.of();
        }
    }

    private static Set<String> getNodeIdsProcessingQuery(DistributedQueryRunner queryRunner, StagesInfo stages)
    {
        return stages.getStages().stream()
                .map(StageInfo::getTasks)
                .flatMap(Collection::stream)
                .map(TaskInfo::taskStatus)
                .map(TaskStatus::getNodeId)
                .filter(nodeId -> isNotCoordinator(queryRunner, nodeId))
                .collect(toImmutableSet());
    }

    private static boolean isNotCoordinator(DistributedQueryRunner queryRunner, String nodeId)
    {
        String coordinatorId = queryRunner.getCoordinator().getCurrentNode().getNodeIdentifier();
        return !nodeId.equalsIgnoreCase(coordinatorId);
    }
}
