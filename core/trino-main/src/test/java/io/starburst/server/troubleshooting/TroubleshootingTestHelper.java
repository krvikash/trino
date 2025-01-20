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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Key;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.execution.QueryInfo;
import io.trino.execution.StageInfo;
import io.trino.execution.TaskInfo;
import io.trino.execution.TaskStatus;
import io.trino.metadata.InternalNodeManager;
import io.trino.spi.QueryId;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.TestingTrinoClient;
import org.intellij.lang.annotations.Language;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

class TroubleshootingTestHelper
{
    private static final Logger log = Logger.get(TroubleshootingTestHelper.class);

    private TroubleshootingTestHelper() {}

    public static Unzipped zipInputStreamToMap(InputStream inputStream, Path tmpDir)
    {
        try {
            ImmutableMap.Builder<String, byte[]> mapBuilder = ImmutableMap.builder();
            Path tmpFile = Files.createTempFile(tmpDir, "troubleshooting-", ".zip");
            log.debug("File with troubleshooting information was stored at: %s", tmpFile);
            try (var outputStream = Files.newOutputStream(tmpFile)) {
                inputStream.transferTo(outputStream);
            }
            try (var zipFile = new ZipFile(tmpFile.toFile())) {
                var e = zipFile.entries();
                while (e.hasMoreElements()) {
                    ZipEntry entry = e.nextElement();
                    mapBuilder.put(entry.getName(), zipFile.getInputStream(entry).readAllBytes());
                }
                return new Unzipped(mapBuilder.buildOrThrow(), tmpFile);
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static Unzipped getTroubleshootingDataForQuery(DistributedQueryRunner queryRunner, Session session, @Language("SQL") String query, Path tmpDir)
            throws Exception
    {
        try (TestingTrinoClient client = new TestingTrinoClient(queryRunner.getCoordinator(), session)) {
            TroubleshootingContextManager troubleshootingContextManager = queryRunner.getCoordinator().getInstance(Key.get(TroubleshootingContextManager.class));
            InputStream inputStream = troubleshootingContextManager.getArchive(client.execute(query).getQueryId()).orElseThrow().get(10, TimeUnit.SECONDS);
            return zipInputStreamToMap(inputStream, tmpDir);
        }
    }

    public record Unzipped(Map<String, byte[]> contents, Path tmpFile) {}

    public static void assertPropertyExists(byte[] actual, String property)
    {
        assertThat(new String(actual, ISO_8859_1)).contains(property);
    }

    public static void assertJvmConfig(byte[] actual)
    {
        String jvmConfig = String.join("\n", ManagementFactory.getRuntimeMXBean().getInputArguments()) + "\n";
        assertThat(actual).isEqualTo(jvmConfig.getBytes(UTF_8));
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

    public static String findWorkerConfigDirectoryName(Unzipped workerConfigs)
    {
        Set<String> paths = workerConfigs.contents().keySet();
        return paths.stream()
                .filter(path -> path.startsWith("worker-") && path.endsWith("/"))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("No worker config directory found. Scanned paths: %s.".formatted(paths)));
    }

    public static Set<String> getNodesProcessingQuery(DistributedQueryRunner queryRunner, QueryId queryId)
    {
        try {
            QueryInfo queryInfo = queryRunner.getCoordinator().getQueryManager().getFullQueryInfo(queryId);
            return queryInfo.getOutputStage()
                    .map(stage -> getNodeIdsProcessingQuery(queryRunner, stage))
                    .orElse(ImmutableSet.of());
        }
        catch (Exception e) {
            return Set.of();
        }
    }

    private static Set<String> getNodeIdsProcessingQuery(DistributedQueryRunner queryRunner, StageInfo outputStage)
    {
        List<TaskInfo> tasks = gatherAllTasks(outputStage);

        return tasks.stream()
                .map(TaskInfo::taskStatus)
                .map(TaskStatus::getNodeId)
                .filter(nodeId -> isNotCoordinator(queryRunner, nodeId))
                .collect(toImmutableSet());
    }

    private static List<TaskInfo> gatherAllTasks(StageInfo stageInfo)
    {
        ImmutableList.Builder<TaskInfo> builder = ImmutableList.builder();
        builder.addAll(stageInfo.getTasks());
        for (StageInfo subStage : stageInfo.getSubStages()) {
            builder.addAll(gatherAllTasks(subStage));
        }
        return builder.build();
    }

    private static boolean isNotCoordinator(DistributedQueryRunner queryRunner, String nodeId)
    {
        String coordinatorId = queryRunner.getCoordinator().getInstance(Key.get(InternalNodeManager.class)).getCurrentNode().getNodeIdentifier();
        return !nodeId.equalsIgnoreCase(coordinatorId);
    }
}
