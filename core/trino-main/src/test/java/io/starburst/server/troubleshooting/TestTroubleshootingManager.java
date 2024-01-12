/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.troubleshooting;

import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import io.starburst.server.troubleshooting.providers.TroubleshootingProvider;
import io.airlift.bootstrap.Bootstrap;
import io.trino.execution.QueryInfo;
import io.trino.spi.QueryId;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.InputStream;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.starburst.server.troubleshooting.providers.TroubleshootingProvider.toInputStream;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.Executors.newScheduledThreadPool;

@ExtendWith(SoftAssertionsExtension.class)
public class TestTroubleshootingManager
{
    @Test
    public void shouldIgnoreThrowingProvidersOnInputStreams(SoftAssertions softly)
            throws Exception
    {
        AtomicBoolean onContextStartedCalled = new AtomicBoolean();
        AtomicBoolean onContextFinishedCalled = new AtomicBoolean();

        TroubleshootingManager manager = new Bootstrap(new AbstractModule()
        {
            @Override
            protected void configure()
            {
                bind(FullQueryInfoProvider.class).to(FullQueryInfoProviderTesting.class).in(Scopes.SINGLETON);
                bind(TroubleshootingManager.class).in(Scopes.SINGLETON);
                bind(ScheduledExecutorService.class).annotatedWith(ForTroubleshooting.class)
                        .toInstance(newScheduledThreadPool(1, daemonThreadsNamed("query-troubleshooting-%s")));

                Multibinder<TroubleshootingProvider> setBinder = newSetBinder(binder(), TroubleshootingProvider.class);
                setBinder.addBinding().to(ThrowingProvider.class).in(Scopes.SINGLETON);
                setBinder.addBinding().toInstance(new HappyPathProvider(onContextStartedCalled, onContextFinishedCalled));
            }
        })
                .initialize()
                .getInstance(TroubleshootingManager.class);

        QueryId queryId = new QueryId("123");
        manager.start(queryId);
        manager.finish(queryId);
        Map<String, InputStream> result = manager.getInputStreams(queryId).get();

        softly.assertThat(result.get("happy").readAllBytes()).isEqualTo("path".getBytes(UTF_8));
        softly.assertThat(onContextStartedCalled.get()).isTrue();
        softly.assertThat(onContextFinishedCalled.get()).isTrue();
    }

    private static class FullQueryInfoProviderTesting
            implements FullQueryInfoProvider
    {
        @Override
        public Optional<QueryInfo> getFullQueryInfo(QueryId queryId)
        {
            return Optional.empty();
        }
    }

    private static class ThrowingProvider
            implements TroubleshootingProvider
    {
        @Override
        public Map<String, InputStream> getInputStreams(TroubleshootingContext context)
        {
            throw new IllegalStateException();
        }

        @Override
        public void onContextStarted(TroubleshootingContext context)
        {
            throw new IllegalStateException();
        }

        @Override
        public void onContextFinished(TroubleshootingContext context)
        {
            throw new IllegalStateException();
        }
    }

    private static class HappyPathProvider
            implements TroubleshootingProvider
    {
        private AtomicBoolean onContextStartCalled;
        private AtomicBoolean onContextFinishedCalled;

        public HappyPathProvider(AtomicBoolean onContextStartCalled, AtomicBoolean onContextFinishedCalled)
        {
            this.onContextStartCalled = onContextStartCalled;
            this.onContextFinishedCalled = onContextFinishedCalled;
        }

        @Override
        public void onContextStarted(TroubleshootingContext context)
        {
            onContextStartCalled.set(true);
        }

        @Override
        public void onContextFinished(TroubleshootingContext context)
        {
            onContextFinishedCalled.set(true);
        }

        @Override
        public Map<String, InputStream> getInputStreams(TroubleshootingContext context)
        {
            return ImmutableMap.of("happy", toInputStream("path"));
        }
    }
}
