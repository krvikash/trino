/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.trino.exchange;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.plugin.base.jmx.MBeanServerModule;
import io.trino.plugin.base.jmx.PrefixObjectNameGeneratorModule;
import io.trino.spi.exchange.ExchangeManager;
import io.trino.spi.exchange.ExchangeManagerFactory;
import org.weakref.jmx.guice.MBeanModule;

import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public class BufferExchangeManagerFactory
        implements ExchangeManagerFactory
{
    private static final Logger log = Logger.get(BufferExchangeManagerFactory.class);

    private static final Duration MAX_WAIT_ACTIVE_NODES = Duration.succinctDuration(10, SECONDS);

    private final Optional<ApiFactory> apiFactory;

    public static BufferExchangeManagerFactory forRealBufferService()
    {
        return new BufferExchangeManagerFactory(Optional.empty());
    }

    @VisibleForTesting
    public static BufferExchangeManagerFactory withApiFactory(ApiFactory apiFactory)
    {
        return new BufferExchangeManagerFactory(Optional.of(apiFactory));
    }

    private BufferExchangeManagerFactory(Optional<ApiFactory> apiFactory)
    {
        this.apiFactory = apiFactory;
    }

    @Override
    public String getName()
    {
        return "buffer";
    }

    @Override
    public ExchangeManager create(Map<String, String> config)
    {
        requireNonNull(config, "config is null");

        Bootstrap app = new Bootstrap(
                new MBeanModule(),
                new MBeanServerModule(),
                new PrefixObjectNameGeneratorModule("io.starburst.stargate.buffer.trino.exchange", "io.starburst.buffer.exchange"),
                new BufferExchangeModule(apiFactory));

        Injector injector = app
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(config)
                .initialize();

        waitActiveNodes(injector);

        return injector.getInstance(BufferExchangeManager.class);
    }

    private static void waitActiveNodes(Injector injector)
    {
        // wait for some time until we see active data nodes; this limits the chance
        // that initial queries would fail if Trino was started just after buffer service cluster and
        // buffer service cluster is not yet ready to serve requests.
        //
        // Also helps with flaky unit tests :)
        BufferNodeDiscoveryManager discoveryManager = injector.getInstance(BufferNodeDiscoveryManager.class);
        long waitStart = System.currentTimeMillis();
        log.info("WAITING for up to " + MAX_WAIT_ACTIVE_NODES + " for ACTIVE buffer service data nodes");
        while (discoveryManager.getBufferNodes().getActiveBufferNodes().isEmpty()) {
            if (System.currentTimeMillis() - waitStart > MAX_WAIT_ACTIVE_NODES.toMillis()) {
                break;
            }
            try {
                Thread.sleep(1000);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
            try {
                discoveryManager.forceRefresh().get(1, SECONDS);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
            catch (Exception e) {
                log.warn(e, "Error refreshing discovery manager");
            }
        }
    }
}
