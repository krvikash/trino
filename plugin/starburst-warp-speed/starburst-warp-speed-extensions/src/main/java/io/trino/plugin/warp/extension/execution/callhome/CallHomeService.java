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
package io.trino.plugin.warp.extension.execution.callhome;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.trino.plugin.warp.annotation.ForWarp;
import io.trino.plugin.warp.cloudvendors.CloudVendorService;
import io.trino.plugin.warp.cloudvendors.config.CloudVendorConfig;
import io.trino.plugin.warp.cloudvendors.config.StoreType;
import io.trino.plugin.warp.extension.config.CallHomeConfig;
import io.trino.plugin.warp.storage.engine.ConnectorSyncInitializedEvent;
import io.trino.plugin.warp.tools.CatalogNameProvider;
import io.trino.plugin.warp.tools.util.StringUtils;
import io.trino.plugin.warp.tools.util.Version;
import io.trino.spi.HostAddress;
import io.trino.spi.NodeManager;
import jakarta.annotation.PreDestroy;
import jakarta.ws.rs.core.UriBuilder;

import java.lang.management.ManagementFactory;
import java.nio.charset.Charset;
import java.time.Instant;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

@Singleton
public class CallHomeService
{
    private static final Logger logger = Logger.get(CallHomeService.class);

    private static final String CALL_HOME_STORE_PATH_PREFIX = "/call-home/";
    private static final String LOG_PATH_PROPERTY_KEY1 = "log.output-file";
    private static final String LOG_PATH_PROPERTY_KEY2 = "node.server-log-file";
    private static final String DEFAULT_CATALOG_PATH = "/etc/presto/catalog";
    private static final String DEFAULT_SERVER_LOG_PATH = "/var/log/presto/server.log";
    static final String CATALOG_PROPERTY_KEY = "catalog.config-dir";
    private final ScheduledExecutorService scheduledExecutorService;
    private final NodeManager nodeManager;
    private final CloudVendorConfig cloudVendorConfig;
    private final CallHomeConfig callHomeConfig;
    private final CloudVendorService cloudVendorService;
    private final HostAddress currentNodeAddress;
    private final String nodeStorePathPrefix;

    private ScheduledFuture<?> scheduledFuture;

    @SuppressWarnings("unused")
    @Inject
    public CallHomeService(
            NodeManager nodeManager,
            CatalogNameProvider catalogNameProvider,
            @ForWarp CloudVendorConfig cloudVendorConfig,
            CallHomeConfig callHomeConfig,
            @ForWarp CloudVendorService cloudVendorService,
            EventBus eventBus)
    {
        this(nodeManager,
                catalogNameProvider,
                cloudVendorConfig,
                callHomeConfig,
                cloudVendorService,
                eventBus,
                Executors.newSingleThreadScheduledExecutor(r -> {
                    Thread t = Executors.defaultThreadFactory().newThread(r);
                    t.setName("CallHomeService:call-home Thread-" + t.threadId());
                    t.setDaemon(true);
                    return t;
                }));
    }

    @VisibleForTesting
    public CallHomeService(
            NodeManager nodeManager,
            CatalogNameProvider catalogNameProvider,
            CloudVendorConfig cloudVendorConfig,
            CallHomeConfig callHomeConfig,
            CloudVendorService cloudVendorService,
            EventBus eventBus,
            ScheduledExecutorService scheduledExecutorService)
    {
        this.nodeManager = requireNonNull(nodeManager);
        this.cloudVendorConfig = requireNonNull(cloudVendorConfig);
        this.callHomeConfig = requireNonNull(callHomeConfig);
        this.cloudVendorService = requireNonNull(cloudVendorService);
        this.currentNodeAddress = nodeManager.getCurrentNode().getHostAndPort();
        this.nodeStorePathPrefix = StringUtils.isNotEmpty(cloudVendorConfig.getStorePath()) ? UriBuilder.fromPath(cloudVendorConfig.getStorePath()).path(catalogNameProvider.get()).path(CALL_HOME_STORE_PATH_PREFIX).path(nodeManager.getCurrentNode().getNodeIdentifier()).build().toString() : null;
        eventBus.register(this);
        this.scheduledExecutorService = requireNonNull(scheduledExecutorService);
    }

    @SuppressWarnings("unused")
    @Subscribe
    public void connectorSyncInitialized(ConnectorSyncInitializedEvent event)
    {
        if (event.isDefaultCatalog() && callHomeConfig.isEnable() && cloudVendorConfig.getStoreType() != StoreType.LOCAL && this.nodeStorePathPrefix != null) {
            logger.debug("scheduling call-home every %s seconds", callHomeConfig.getIntervalInSeconds());
            uploadNodeInfo();
            scheduledFuture = scheduledExecutorService.scheduleAtFixedRate(
                    new CallHomeJob(
                            cloudVendorService,
                            currentNodeAddress,
                            nodeStorePathPrefix,
                            getServerLogPath(),
                            getCatalogPath(),
                            false),
                    callHomeConfig.getIntervalInSeconds(),
                    callHomeConfig.getIntervalInSeconds(),
                    TimeUnit.SECONDS);
        }
        else {
            logger.debug("call-home is disabled for this connector");
        }
    }

    private void uploadNodeInfo()
    {
        try {
            Instant uptime = Instant.ofEpochMilli(ManagementFactory.getRuntimeMXBean().getStartTime());
            StringJoiner nodeInfo = new StringJoiner("\n");
            nodeInfo.add("host-ip=" + nodeManager.getCurrentNode().getHost());
            nodeInfo.add("version=" + Version.getInstance().getVersion());
            nodeInfo.add("is-coordinator=" + nodeManager.getCurrentNode().isCoordinator());
            nodeInfo.add("node-id" + nodeManager.getCurrentNode().getNodeIdentifier());
            nodeInfo.add("uptime=" + uptime.toString());

            cloudVendorService.uploadToCloud(
                    nodeInfo.toString().getBytes(Charset.defaultCharset()),
                    CloudVendorService.concatenatePath(nodeStorePathPrefix, uptime.toString(), "-node.info"));
        }
        catch (Exception e) {
            logger.warn(e, "failed to store node info file");
        }
    }

    public synchronized Optional<Integer> triggerCallHome(String storePath, boolean collectThreadDump, boolean waitToFinish)
    {
        if (cloudVendorConfig.getStoreType() == StoreType.LOCAL) {
            logger.debug("call-home is disabled for this connector since it's using local store");
            return Optional.empty();
        }

        if ((scheduledFuture == null) || (scheduledFuture.getDelay(TimeUnit.SECONDS) > 0)) {
            logger.debug("storePath = %s, nodeStorePathPrefix=%s", storePath, nodeStorePathPrefix);
            CallHomeJob callHomeJob = new CallHomeJob(
                    cloudVendorService,
                    currentNodeAddress,
                    (storePath != null) ? storePath : nodeStorePathPrefix,
                    getServerLogPath(),
                    getCatalogPath(),
                    collectThreadDump);
            if (waitToFinish) {
                callHomeJob.run();
                return Optional.of(callHomeJob.getNumberOfUploaded());
            }
            else {
                Future<?> _ = scheduledExecutorService.schedule(callHomeJob, 0, TimeUnit.SECONDS);
            }
        }
        return Optional.empty();
    }

    @PreDestroy
    public void shutdown()
    {
        try {
            triggerCallHome(null, false, true);
        }
        catch (Exception e) {
            logger.warn("failed to call-home on shutdown");
        }
    }

    private String getCatalogPath()
    {
        return System.getProperty(CATALOG_PROPERTY_KEY, DEFAULT_CATALOG_PATH);
    }

    private static String getServerLogPath()
    {
        return System.getProperty(
                LOG_PATH_PROPERTY_KEY1,
                System.getProperty(LOG_PATH_PROPERTY_KEY2, DEFAULT_SERVER_LOG_PATH));
    }
}
