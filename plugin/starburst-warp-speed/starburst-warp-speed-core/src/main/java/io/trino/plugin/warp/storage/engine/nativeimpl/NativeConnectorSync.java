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
package io.trino.plugin.warp.storage.engine.nativeimpl;

import com.google.common.eventbus.EventBus;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.trino.plugin.warp.dispatcher.warmup.demoter.WarmupDemoterService;
import io.trino.plugin.warp.gen.constants.DemoteStatus;
import io.trino.plugin.warp.storage.engine.ConnectorSync;
import io.trino.plugin.warp.storage.engine.ConnectorSyncInitializedEvent;
import io.trino.plugin.warp.storage.read.StorageCollectorCallBack;
import io.trino.spi.catalog.CatalogName;
import jakarta.annotation.PreDestroy;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.Objects.requireNonNull;

@Singleton
public class NativeConnectorSync
        implements ConnectorSync
{
    private static final Logger logger = Logger.get(NativeConnectorSync.class);
    private final CatalogName catalogName;
    private final EventBus eventBus;
    private final ExecutorService executorService = Executors.newSingleThreadExecutor(daemonThreadsNamed("warp-speed-native-connector-sync-%s"));
    private WarmupDemoterService warmupDemoterService;
    private Integer catalogSequence;

    @Inject
    public NativeConnectorSync(CatalogName catalogName,
                               EventBus eventBus)
    {
        this.catalogName = catalogName;
        this.eventBus = requireNonNull(eventBus);
    }

    @Override
    public void setWarmupDemoterService(WarmupDemoterService warmupDemoterService)
    {
        this.warmupDemoterService = warmupDemoterService;
        init();
    }

    @PreDestroy
    public void shutdown()
    {
        try {
            logger.debug("nativeConnectorSync from shutdown, %d", System.identityHashCode(this));
            if (!unregister(catalogSequence)) {
                logger.error("failed to unregister");
                return;
            }
            logger.info("unregister catalog name %s sequence %d", catalogName, catalogSequence);
        }
        catch (Throwable e) {
            logger.error(e, "failed to unregister");
        }
        finally {
            logger.debug("unregister finally");
        }
    }

    public void init()
    {
        try {
            logger.debug("nativeConnectorSync from init, %d", System.identityHashCode(this));
            catalogSequence = register(catalogName.toString(), StorageCollectorCallBack.class, 1800);
            logger.info("catalog name %s sequence %d", catalogName, catalogSequence);
            eventBus.post(new ConnectorSyncInitializedEvent(catalogSequence));
        }
        catch (Throwable e) {
            logger.error(e, "failed to register");
            throw new RuntimeException(e);
        }
        finally {
            logger.debug("register finally");
        }
    }

    public void callback_GetLowestPriority(int demoteSequence)
    {
        logger.debug("%s - callback_GetLowestPriority, demoteSequence = %d",
                catalogName, demoteSequence);
        if (warmupDemoterService == null) {
            logger.error("warmupDemoterCatalogService == null");
            return;
        }
        Future<?> unused = executorService.submit(() -> warmupDemoterService.connectorSyncStartDemote(demoteSequence));
    }

    public void callback_DemoteStart(int demoteSequence, double maxPriorityToDemote, boolean isSingleConnector)
    {
        logger.debug("%s - callback_DemoteStart, demoteSequence=%d, maxPriorityToDemote=%f, isSingleConnector=%b",
                catalogName, demoteSequence, maxPriorityToDemote, isSingleConnector);
        if (warmupDemoterService == null) {
            logger.error("warmupDemoterCatalogService == null");
            return;
        }
        Future<?> unused = executorService.submit(() -> warmupDemoterService.connectorSyncStartDemoteCycle(maxPriorityToDemote, isSingleConnector));
    }

    public void callback_DemoteEnd(int demoteSequence, double highestPriority)
    {
        logger.debug("%s - callback_demoteEnd, demoteSequence=%d, highestPriority=%f",
                catalogName, demoteSequence, highestPriority);
        if (warmupDemoterService == null) {
            logger.error("%s - warmupDemoterCatalogService == null", catalogName);
            return;
        }
        Future<?> unused = executorService.submit(() -> warmupDemoterService.connectorSyncDemoteEnd(demoteSequence, highestPriority));
    }

    public native int register(String catalogName, Class<StorageCollectorCallBack> storageCollector, int timeoutSec);

    public native boolean unregister(int connectorId);

    @Override
    public void startDemote(int demoteSequence)
    {
        logger.debug("%s - call syncDemoteStart with demoteSequence =%d", catalogName, demoteSequence);
        syncDemoteStart(catalogSequence, demoteSequence);
    }

    @Override
    public void syncDemoteCycleEnd(int demoteSequence, double lowestPriorityExist, double highestPriorityDemoted, DemoteStatus demoteStatus)
    {
        logger.debug("%s -call syncDemoteCycleEnd with demoteSequence=%d, lowestPriorityExist=%f, highestPriorityDemoted=%f, demoteStatus=%s, demoteStatusOrdinal=%d",
                catalogName, demoteSequence, lowestPriorityExist, highestPriorityDemoted, demoteStatus.name(), demoteStatus.ordinal());
        syncDemoteCycleEnd(catalogSequence, demoteSequence, lowestPriorityExist, highestPriorityDemoted, demoteStatus.ordinal());
    }

    @Override
    public int syncDemotePrepare(double epsilon)
    {
        logger.debug("%s -call syncDemotePrepare with epsilon=%f",
                catalogName, epsilon);
        return syncDemotePrepare(catalogSequence, epsilon);
    }

    private native int syncDemotePrepare(int connectorId, double epsilon);

    private native void syncDemoteStart(int catalogSequence, int demoteSequence);

    private native void syncDemoteCycleEnd(int catalogSequence, int demoteSequence, double lowestPriorityExist, double highestPriorityDemoted, int demoteStatus);

    @Override
    public int getCatalogSequence()
    {
        return catalogSequence;
    }

    @Override
    public String getCatalogName()
    {
        return catalogName.toString();
    }

    @Override
    public boolean isCatalogReducedResources()
    {
        return catalogSequence >= 4;
    }
}
