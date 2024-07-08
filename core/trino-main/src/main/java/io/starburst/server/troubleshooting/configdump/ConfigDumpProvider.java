/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.troubleshooting.configdump;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.starburst.server.troubleshooting.DownloadResult;
import io.starburst.server.troubleshooting.TroubleshootingContext;
import io.starburst.server.troubleshooting.providers.TroubleshootingProvider;
import io.trino.metadata.InternalNode;
import io.trino.metadata.InternalNodeManager;

import java.io.InputStream;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class ConfigDumpProvider
        implements TroubleshootingProvider
{
    private final ConfigDumper configDumper;
    private final InternalNodeManager nodeManager;
    private final RemoteConfigDumpClient remoteConfigDumpClient;

    @Inject
    public ConfigDumpProvider(ConfigDumper configDumper, InternalNodeManager nodeManager, RemoteConfigDumpClient remoteConfigDumpClient)
    {
        this.configDumper = requireNonNull(configDumper, "configDumper is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.remoteConfigDumpClient = requireNonNull(remoteConfigDumpClient, "remoteConfigDumpClient is null");
    }

    @Override
    public Map<String, InputStream> getInputStreams(TroubleshootingContext context)
    {
        ImmutableMap.Builder<String, InputStream> inputStreamsBuilder = ImmutableMap.<String, InputStream>builder()
                .put("configs/coordinator.zip", configDumper.dumpLocalConfig());
        Optional<InternalNode> workerNode = selectWorkerNode(context.getQueryCollectedNodes());
        workerNode.ifPresent(node -> {
            DownloadResult remoteDownloadResult = remoteConfigDumpClient.download(node);
            if (remoteDownloadResult.isSuccessful()) {
                inputStreamsBuilder.put("configs/worker-" + node.getNodeIdentifier() + ".zip", remoteDownloadResult.inputStream());
            }
            else {
                inputStreamsBuilder.put("configs/worker-" + node.getNodeIdentifier() + ".error.txt", remoteDownloadResult.inputStream());
            }
        });
        return inputStreamsBuilder.buildOrThrow();
    }

    private Optional<InternalNode> selectWorkerNode(Set<String> allProcessingNodes)
    {
        return nodeManager.getAllNodes()
                .getActiveNodes()
                .stream()
                .filter(node -> allProcessingNodes.contains(node.getNodeIdentifier()))
                .filter(node -> !node.equals(nodeManager.getCurrentNode()))
                .findFirst();
    }
}
