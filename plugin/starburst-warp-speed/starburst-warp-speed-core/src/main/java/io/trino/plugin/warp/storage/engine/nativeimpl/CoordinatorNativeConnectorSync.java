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
import jakarta.annotation.PreDestroy;

import static java.util.Objects.requireNonNull;

@Singleton
public class CoordinatorNativeConnectorSync
        implements ConnectorSync
{
    private static final Logger logger = Logger.get(CoordinatorNativeConnectorSync.class);
    private final EventBus eventBus;

    @Inject
    public CoordinatorNativeConnectorSync(EventBus eventBus)
    {
        this.eventBus = requireNonNull(eventBus);
        eventBus.register(this);
    }

    @PreDestroy
    public void shutdown()
    {
    }

    @Subscribe
    public void init(CoordinatorInitializedEvent coordinatorInitializedEvent)
    {
        try {
            eventBus.post(new ConnectorSyncInitializedEvent(true));
        }
        catch (Throwable e) {
            logger.error(e, "failed to register");
            throw new RuntimeException(e);
        }
    }
}
