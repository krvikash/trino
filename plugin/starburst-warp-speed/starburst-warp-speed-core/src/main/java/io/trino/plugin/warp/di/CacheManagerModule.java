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
package io.trino.plugin.warp.di;

import com.google.inject.Binder;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.name.Names;
import io.trino.plugin.warp.dispatcher.cache.AbortAction;
import io.trino.plugin.warp.dispatcher.cache.AbortOnInitAction;
import io.trino.plugin.warp.dispatcher.cache.CacheAction;
import io.trino.plugin.warp.dispatcher.cache.EmptyPageAction;
import io.trino.plugin.warp.dispatcher.cache.FinishAction;
import io.trino.plugin.warp.dispatcher.cache.ParallelWarmUpLimiter;
import io.trino.plugin.warp.dispatcher.cache.WorkerCacheManager;
import io.trino.plugin.warp.dispatcher.warmup.CacheWarmState;
import io.trino.plugin.warp.dispatcher.warmup.warmers.CacheWarmer;
import io.trino.plugin.warp.storage.write.WarpCacheFilesMerger;
import io.trino.spi.cache.CacheManager;
import io.trino.spi.connector.ConnectorContext;

import java.util.Map;

public class CacheManagerModule
        implements VaradaBaseModule
{
    private ConnectorContext context;
    private Map<String, String> config;

    public CacheManagerModule(Map<String, String> config, ConnectorContext context)
    {
        this.config = config;
        this.context = context;
    }

    @Override
    public void configure(Binder binder)
    {
        if (VaradaBaseModule.isWorker(context, config)) {
            binder.bind(CacheManager.class).to(WorkerCacheManager.class);
            binder.bind(CacheWarmer.class);
            binder.bind(WarpCacheFilesMerger.class);
            binder.bind(ParallelWarmUpLimiter.class);
            MapBinder<CacheWarmState, CacheAction> mapBinder = MapBinder.newMapBinder(binder,
                    CacheWarmState.class, CacheAction.class, Names.named("CacheActions"));
            mapBinder.addBinding(CacheWarmState.FINISHING).to(FinishAction.class);
            mapBinder.addBinding(CacheWarmState.EMPTY_PAGE).to(EmptyPageAction.class);
            mapBinder.addBinding(CacheWarmState.ABORT_ON_INIT_PROCESS).to(AbortOnInitAction.class);
            mapBinder.addBinding(CacheWarmState.ABORTING).to(AbortAction.class);
        }
    }

    @Override
    public CacheManagerModule withConfig(Map<String, String> config)
    {
        this.config = config;
        return this;
    }

    @Override
    public CacheManagerModule withContext(ConnectorContext context)
    {
        this.context = context;
        return this;
    }
}
