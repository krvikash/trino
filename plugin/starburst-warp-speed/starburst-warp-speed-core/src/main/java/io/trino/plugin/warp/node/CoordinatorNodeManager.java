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
package io.trino.plugin.warp.node;

import com.google.common.eventbus.EventBus;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.di.WarpInitializedServiceRegistry;
import io.trino.plugin.warp.gen.stats.CachePredicatesStats;
import io.trino.plugin.warp.gen.stats.DispatcherPageSourceStats;
import io.trino.plugin.warp.gen.stats.WarmingServiceStats;
import io.trino.plugin.warp.gen.stats.WarmupDemoterStats;
import io.trino.plugin.warp.gen.stats.WarmupExportServiceStats;
import io.trino.plugin.warp.gen.stats.WarmupImportServiceStats;
import io.trino.plugin.warp.gen.stats.WorkerTaskExecutorServiceStats;
import io.trino.plugin.warp.metrics.MetricsManager;
import io.trino.plugin.warp.util.WarpInitializedServiceMarker;
import io.trino.spi.Node;
import io.trino.spi.NodeManager;

import java.util.Comparator;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

@Singleton
public class CoordinatorNodeManager
        implements WarpInitializedServiceMarker
{
    private static final Logger logger = Logger.get(CoordinatorNodeManager.class);

    private final NodeManager nodeManager;
    private final GlobalConfig globalConfig;
    private final EventBus eventBus; // required for @Subscribe methods
    private final MetricsManager metricsManager;
    private boolean coordinatorInitialized; // no need to set, default is false

    @Inject
    public CoordinatorNodeManager(
            NodeManager nodeManager,
            GlobalConfig globalConfig,
            EventBus eventBus,
            MetricsManager metricsManager,
            WarpInitializedServiceRegistry warpInitializedServiceRegistry)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.globalConfig = requireNonNull(globalConfig, "globalConfig is null");
        this.eventBus = requireNonNull(eventBus, "EventBus is null");
        this.metricsManager = requireNonNull(metricsManager);
        warpInitializedServiceRegistry.addService(this);
    }

    @Override
    public void init()
    {
        logger.debug("coordinator node [%s] is initialising", getCoordinatorNode().getNodeIdentifier());

        metricsManager.registerMetric(DispatcherPageSourceStats.create());
        metricsManager.registerMetric(WarmingServiceStats.create());
        metricsManager.registerMetric(WarmupDemoterStats.create());
        metricsManager.registerMetric(WarmupImportServiceStats.create());
        metricsManager.registerMetric(WarmupExportServiceStats.create());
        metricsManager.registerMetric(WorkerTaskExecutorServiceStats.create());
        metricsManager.registerMetric(CachePredicatesStats.create());

        coordinatorInitialized = true;

        eventBus.post(new CoordinatorInitializedEvent(getCoordinatorNode()));
    }

    public Node getCoordinatorNode()
    {
        return nodeManager.getCurrentNode();
    }

    public List<Node> getWorkerNodes()
    {
        List<Node> workers;
        if (globalConfig.getIsSingle()) {
            workers = List.of(nodeManager.getCurrentNode());
        }
        else {
            workers = nodeManager
                    .getAllNodes()
                    .stream()
                    .filter(node -> !node.isCoordinator())
                    .sorted(Comparator.comparing(Node::getNodeIdentifier))
                    .collect(toImmutableList());
        }
        return workers;
    }

    public boolean isReady()
    {
        return coordinatorInitialized;
    }

    public boolean isClusterReady()
    {
        logger.debug(
                "isClusterReady::isCoordinatorReady=%b workerNodes=%s",
                isReady(),
                nodeManager.getWorkerNodes());
        return isReady() &&
                (!nodeManager.getWorkerNodes().isEmpty() || globalConfig.getIsSingle());
    }
}
