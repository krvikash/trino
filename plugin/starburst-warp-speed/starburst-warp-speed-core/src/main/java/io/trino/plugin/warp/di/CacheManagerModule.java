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
import com.google.inject.Module;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.name.Names;
import io.trino.plugin.warp.dispatcher.cache.AbortAction;
import io.trino.plugin.warp.dispatcher.cache.AbortOnEngineAction;
import io.trino.plugin.warp.dispatcher.cache.AbortOnInitAction;
import io.trino.plugin.warp.dispatcher.cache.CacheAction;
import io.trino.plugin.warp.dispatcher.cache.CoordinatorCacheManager;
import io.trino.plugin.warp.dispatcher.cache.EmptyPageAction;
import io.trino.plugin.warp.dispatcher.cache.FinishAction;
import io.trino.plugin.warp.dispatcher.cache.MemoryContextService;
import io.trino.plugin.warp.dispatcher.cache.WorkerCacheManager;
import io.trino.plugin.warp.dispatcher.warmup.CacheWarmState;
import io.trino.plugin.warp.dispatcher.warmup.warmers.CacheWarmer;
import io.trino.plugin.warp.storage.write.WarpCacheFilesMerger;
import io.trino.spi.cache.CacheManager;
import io.trino.spi.cache.CacheManagerContext;

public class CacheManagerModule
        implements Module
{
    private final CacheManagerContext context;
    private final boolean isCoordinator;

    public CacheManagerModule(CacheManagerContext context, boolean isCoordinator)
    {
        this.context = context;
        this.isCoordinator = isCoordinator;
    }

    @Override
    public void configure(Binder binder)
    {
        if (isCoordinator) {
            binder.bind(CacheManager.class).to(CoordinatorCacheManager.class);
            return;
        }
        binder.bind(CacheManager.class).to(WorkerCacheManager.class);
        binder.bind(CacheWarmer.class);
        binder.bind(WarpCacheFilesMerger.class);
        binder.bind(MemoryContextService.class);
        binder.bind(CacheManagerContext.class).toInstance(context);
        MapBinder<CacheWarmState, CacheAction> mapBinder = MapBinder.newMapBinder(binder,
                CacheWarmState.class, CacheAction.class, Names.named("CacheActions"));

        mapBinder.addBinding(CacheWarmState.FINISHING).to(FinishAction.class);
        mapBinder.addBinding(CacheWarmState.EMPTY_PAGE).to(EmptyPageAction.class);
        mapBinder.addBinding(CacheWarmState.ABORT_ON_INIT_PROCESS).to(AbortOnInitAction.class);
        mapBinder.addBinding(CacheWarmState.ABORTING).to(AbortAction.class);
        mapBinder.addBinding(CacheWarmState.ABORT_FROM_ENGINE).to(AbortOnEngineAction.class);
    }
}
