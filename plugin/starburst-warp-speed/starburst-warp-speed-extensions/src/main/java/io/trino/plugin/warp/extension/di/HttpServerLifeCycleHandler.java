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
package io.trino.plugin.warp.extension.di;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.http.server.HttpServer;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.log.Logger;
import io.trino.plugin.warp.di.WarpInitializedServiceRegistry;
import io.trino.plugin.warp.util.UriUtils;
import io.trino.plugin.warp.util.WarpInitializedServiceMarker;
import io.trino.spi.NodeManager;

import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;

@Singleton
public class HttpServerLifeCycleHandler
        implements WarpInitializedServiceMarker
{
    private static final Logger logger = Logger.get(HttpServerLifeCycleHandler.class);

    private static final Map<Integer, Integer> trinoToWarpPort = new ConcurrentHashMap<>();

    private final HttpServer httpServer;
    private final LifeCycleManager lifeCycleManager;

    @Inject
    public HttpServerLifeCycleHandler(
            HttpServer httpServer,
            HttpServerInfo httpServerInfo,
            LifeCycleManager lifeCycleManager,
            NodeManager nodeManager,
            WarpInitializedServiceRegistry warpInitializedServiceRegistry)
    {
        this.httpServer = requireNonNull(httpServer);
        this.lifeCycleManager = requireNonNull(lifeCycleManager);
        warpInitializedServiceRegistry.addService(this);

        URI httpUri = httpServerInfo.getHttpUri();
        if (httpUri != null) {
            int warpPort = httpUri.getPort();
            int trinoPort = UriUtils.getHttpUri(nodeManager.getCurrentNode()).getPort();
            registerWarpRestPort(trinoPort, warpPort);
            logger.info("Warp extension HTTP server bound to port %d (Trino port %d)", warpPort, trinoPort);
        }
    }

    @Override
    public void init()
    {
        lifeCycleManager.addInstance(httpServer);
    }

    public static void registerWarpRestPort(int trinoPort, int warpPort)
    {
        trinoToWarpPort.put(trinoPort, warpPort);
    }

    public static Optional<Integer> getWarpRestPort(int trinoPort)
    {
        return Optional.ofNullable(trinoToWarpPort.get(trinoPort));
    }
}
