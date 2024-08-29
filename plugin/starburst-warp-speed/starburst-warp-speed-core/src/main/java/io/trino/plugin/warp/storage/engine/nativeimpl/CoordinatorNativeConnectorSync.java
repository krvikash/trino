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
import com.google.common.eventbus.Subscribe;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.trino.plugin.warp.node.CoordinatorInitializedEvent;
import io.trino.plugin.warp.storage.engine.ConnectorSync;
import io.trino.plugin.warp.storage.engine.ConnectorSyncInitializedEvent;
import io.trino.spi.catalog.CatalogName;
import jakarta.annotation.PreDestroy;

import static java.util.Objects.requireNonNull;

@Singleton
public class CoordinatorNativeConnectorSync
        implements ConnectorSync
{
    private static final Logger logger = Logger.get(CoordinatorNativeConnectorSync.class);
    private final CatalogName catalogName;
    private final EventBus eventBus;
    private Integer catalogSequence;

    @Inject
    public CoordinatorNativeConnectorSync(CatalogName catalogName,
            EventBus eventBus)
    {
        this.catalogName = catalogName;
        this.eventBus = requireNonNull(eventBus);
        eventBus.register(this);
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

    @Subscribe
    public void init(CoordinatorInitializedEvent coordinatorInitializedEvent)
    {
        try {
            logger.debug("nativeConnectorSync from init, %d", System.identityHashCode(this));
            catalogSequence = register(catalogName.toString(), 1800);
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

    @Override
    public int getCatalogSequence()
    {
        return catalogSequence;
    }

    public native int register(String catalogName, int timeoutSec);

    public native boolean unregister(int connectorId);
}
