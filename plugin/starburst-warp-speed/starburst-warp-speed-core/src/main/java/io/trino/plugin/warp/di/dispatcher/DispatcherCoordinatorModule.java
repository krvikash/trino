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
package io.trino.plugin.warp.di.dispatcher;

import com.google.inject.Binder;
import io.trino.plugin.warp.di.ExtraModule;
import io.trino.plugin.warp.dispatcher.DispatcherCacheMetadata;
import io.trino.plugin.warp.dispatcher.DispatcherMetadataFactory;
import io.trino.plugin.warp.dispatcher.DispatcherSplitManager;
import io.trino.plugin.warp.dispatcher.DispatcherStatisticsProvider;
import io.trino.plugin.warp.dispatcher.DispatcherTransactionManager;
import io.trino.plugin.warp.dispatcher.WarpConnectorContext;
import io.trino.plugin.warp.expression.rewrite.ExpressionService;
import io.trino.plugin.warp.expression.rewrite.coordinator.connectortowarp.SupportedFunctions;
import io.trino.plugin.warp.expression.rewrite.coordinator.warptonative.NativeExpressionRulesHandler;
import io.trino.plugin.warp.storage.splits.ConnectorSplitConsistentHashNodeDistributor;
import io.trino.plugin.warp.storage.splits.ConnectorSplitNodeDistributor;

import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * this module will install dependencies which are required in the coordinator regardless if it is single or not
 */
public class DispatcherCoordinatorModule
        implements ExtraModule
{
    private WarpConnectorContext context;

    public DispatcherCoordinatorModule(Map<String, String> config, WarpConnectorContext context)
    {
        withConfig(config).withContext(context);
    }

    @Override
    public void configure(Binder binder)
    {
        if (context.getCurrentNode().isCoordinator()) {
            binder.bind(DispatcherTransactionManager.class);
            binder.bind(DispatcherStatisticsProvider.class);
            binder.bind(DispatcherMetadataFactory.class);
            binder.bind(DispatcherCacheMetadata.class);
            binder.bind(ExpressionService.class);
            binder.bind(ConnectorSplitNodeDistributor.class).to(ConnectorSplitConsistentHashNodeDistributor.class);
            binder.bind(SupportedFunctions.class);
            binder.bind(NativeExpressionRulesHandler.class);
            binder.bind(DispatcherSplitManager.class);
        }
    }

    @Override
    public boolean shouldInstall()
    {
        return context.getCurrentNode().isCoordinator();
    }

    @Override
    public DispatcherCoordinatorModule withConfig(Map<String, String> config)
    {
        return this;
    }

    @Override
    public DispatcherCoordinatorModule withContext(WarpConnectorContext context)
    {
        this.context = requireNonNull(context);
        return this;
    }
}
