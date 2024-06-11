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
import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import io.starburst.server.troubleshooting.TroubleshootingTestHelper.Unzipped;
import io.starburst.server.troubleshooting.jfr.FlightRecorderConfig;
import io.starburst.server.troubleshooting.providers.TroubleshootingProvider;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.configuration.ConfigDefaults;
import io.airlift.units.Duration;
import io.trino.client.NodeVersion;
import io.trino.execution.QueryInfo;
import io.trino.metadata.InMemoryNodeManager;
import io.trino.metadata.InternalNode;
import io.trino.metadata.InternalNodeManager;
import io.trino.metadata.SessionPropertyManager;
import io.trino.spi.QueryId;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.channels.Channels;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.starburst.server.troubleshooting.TroubleshootingTestHelper.zipInputStreamToMap;
import static io.starburst.server.troubleshooting.providers.TroubleshootingProvider.toInputStream;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.units.Duration.succinctDuration;
import static io.trino.testing.assertions.Assert.assertEventually;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(SoftAssertionsExtension.class)
@Timeout(value = 30)
public class TestTroubleshootingContextManager
{
    @Test
    public void shouldExecuteNonThrowingProvidersAndReportErrorsForThrowingOnes(SoftAssertions softly, @TempDir Path tmpDir)
            throws Exception
    {
        AtomicBoolean onContextStartedCalled = new AtomicBoolean();
        AtomicBoolean onContextFinishedCalled = new AtomicBoolean();
        HappyPathProvider happyPathProvider = new HappyPathProvider(onContextStartedCalled, onContextFinishedCalled);

        TroubleshootingContextManager manager = createTroubleshootingContextManager(config -> {}, new ThrowingProvider(), happyPathProvider);

        QueryId queryId = new QueryId("123");
        manager.start(queryId);
        manager.finish(queryId);

        Unzipped unzipped = zipInputStreamToMap(manager.getArchive(queryId).get(), tmpDir);
        softly.assertThat(unzipped.contents())
                .hasSize(5)
                .hasEntrySatisfying("123/happy", b -> softly.assertThat(new String(b, UTF_8)).isEqualTo("path"))
                .hasEntrySatisfying("123/throwWhileReading.txt", b -> softly.assertThat(b.length).isZero())
                .hasEntrySatisfying("123/largeFile.txt", b -> softly.assertThat(b.length).isEqualTo(1024 * 1024 * 100))
                .hasEntrySatisfying("123/top-level.errors", b -> softly.assertThat(new String(b, UTF_8)).contains(ThrowingProvider.class.getName()))
                .hasEntrySatisfying("123/throwWhileReading.txt.errors", b -> softly.assertThat(new String(b, UTF_8)).contains(HappyPathProvider.class.getName()));

        softly.assertThat(onContextStartedCalled.get()).isTrue();
        softly.assertThat(onContextFinishedCalled.get()).isTrue();
    }

    @Test
    public void shouldRetryRemove()
    {
        AtomicBoolean removeTried = new AtomicBoolean();
        AtomicBoolean removeSucceeded = new AtomicBoolean();
        TroubleshootingProvider shouldRetryRemoveProvider = new TroubleshootingProvider()
        {
            @Override
            public void onContextRemoved(TroubleshootingContext context)
            {
                if (!removeTried.get()) {
                    removeTried.set(true);
                    throw new RuntimeException("remove failed");
                }
                removeSucceeded.set(true);
            }
        };

        TroubleshootingContextManager manager = createTroubleshootingContextManager(config -> {
            config.setMaxAccessDuration(Duration.ZERO);
            config.setCleanupInterval(new Duration(1, MILLISECONDS));
        }, shouldRetryRemoveProvider);

        QueryId queryId = new QueryId("123");
        manager.start(queryId);
        manager.finish(queryId);

        assertEventually(succinctDuration(1, SECONDS), () -> assertThat(removeSucceeded.get()).isTrue());
    }

    private static TroubleshootingContextManager createTroubleshootingContextManager(
            ConfigDefaults<TroubleshootingConfig> troubleshootingConfigDefaults,
            TroubleshootingProvider... providers)
    {
        return new Bootstrap(new AbstractConfigurationAwareModule()
        {
            @Override
            protected void setup(Binder binder)
            {
                configBinder(binder).bindConfig(TroubleshootingConfig.class);
                configBinder(binder).bindConfig(FlightRecorderConfig.class);
                configBinder(binder).bindConfigDefaults(TroubleshootingConfig.class, troubleshootingConfigDefaults);
                binder.bind(FullQueryInfoProvider.class).to(FullQueryInfoProviderTesting.class).in(Scopes.SINGLETON);
                binder.bind(TroubleshootingContextManager.class).in(Scopes.SINGLETON);
                binder.bind(TroubleshootingArchiver.class).in(Scopes.SINGLETON);
                binder.bind(ScheduledExecutorService.class).annotatedWith(ForTroubleshooting.class)
                        .toInstance(newSingleThreadScheduledExecutor(daemonThreadsNamed("query-troubleshooting-%s")));
                binder.bind(InternalNodeManager.class).toInstance(new InMemoryNodeManager(
                        new InternalNode("coordinator", URI.create("http://127.0.0.1:11"), NodeVersion.UNKNOWN, true)));
                binder.bind(SessionPropertyManager.class).in(Scopes.SINGLETON);

                Multibinder<TroubleshootingProvider> setBinder = newSetBinder(binder, TroubleshootingProvider.class);

                for (TroubleshootingProvider provider : providers) {
                    setBinder.addBinding().toInstance(provider);
                }
            }
        })
                .initialize()
                .getInstance(TroubleshootingContextManager.class);
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
            return ImmutableMap.of("happy", toInputStream("path"),
                    "throwWhileReading.txt", new InputStream()
                    {
                        @Override
                        public int read()
                        {
                            throw new IllegalStateException(TestTroubleshootingContextManager.HappyPathProvider.class.getName());
                        }
                    },
                    "largeFile.txt", getLargeFileInputStream());
        }
    }

    private static InputStream getLargeFileInputStream()
    {
        try {
            RandomAccessFile f = new RandomAccessFile("t", "rw");
            f.setLength(1024 * 1024 * 100);
            return Channels.newInputStream(f.getChannel());
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
