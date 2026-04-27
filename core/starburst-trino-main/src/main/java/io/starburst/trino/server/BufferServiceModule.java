/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.trino.server;

import com.google.inject.Binder;
import com.google.inject.multibindings.ProvidesIntoOptional;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.starburst.stargate.buffer.data.server.DrainService;
import io.starburst.stargate.buffer.trino.exchange.BufferExchangeManagerFactory.RealBufferExchangeManagerFactoryModule;
import io.starburst.trino.server.buffer.EmbeddedBufferServiceDataModule;
import io.starburst.trino.server.buffer.EmbeddedBufferServiceDiscoveryModule;
import io.trino.execution.scheduler.NodeSchedulerConfig;
import io.trino.server.NodeStateManager;
import io.trino.server.ServerConfig;
import io.trino.server.buffer.EmbeddedBufferServiceConfig;
import io.trino.server.testing.TestingServerExtensionModule;

import static com.google.inject.multibindings.ProvidesIntoOptional.Type.ACTUAL;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.starburst.stargate.buffer.data.server.DataServerApplicationModules.getSpoolingConfigurationModule;
import static io.trino.server.buffer.EmbeddedBufferServiceConfig.EMBEDDED_BUFFER_SERVICE_CONFIG_PREFIX;

public class BufferServiceModule
        extends AbstractConfigurationAwareModule
        implements TestingServerExtensionModule
{
    @Override
    protected void setup(Binder binder)
    {
        // Buffer service exchange
        configBinder(binder).bindConfig(EmbeddedBufferServiceConfig.class);
        install(new RealBufferExchangeManagerFactoryModule());

        if (!buildConfigObject(EmbeddedBufferServiceConfig.class).isEmbeddedBufferServiceEnabled()) {
            return;
        }

        if (buildConfigObject(ServerConfig.class).isCoordinator()) {
            install(new CoordinatorBufferServiceModule());
        }
        else {
            install(new WorkerBufferServiceModule());
        }
    }

    private static class CoordinatorBufferServiceModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        protected void setup(Binder binder)
        {
            install(new EmbeddedBufferServiceDiscoveryModule());
            if (buildConfigObject(NodeSchedulerConfig.class).isIncludeCoordinator()) {
                // if coordinator is doing worker job start up data server too
                install(new EmbeddedBufferServiceDataModule());
                install(new PreShutdownActionModule());
            }
            else {
                // just bind storage manager configs
                install(getSpoolingConfigurationModule(EMBEDDED_BUFFER_SERVICE_CONFIG_PREFIX));
            }
        }
    }

    private static class WorkerBufferServiceModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        protected void setup(Binder binder)
        {
            install(new EmbeddedBufferServiceDataModule());
            install(new PreShutdownActionModule());
        }
    }

    private static class PreShutdownActionModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        protected void setup(Binder binder)
        {}

        @ProvidesIntoOptional(ACTUAL)
        @NodeStateManager.PreShutdownAction
        public Runnable getDrainPreShutdownAction(DrainService drainService)
        {
            return drainService::awaitDrain;
        }
    }
}
