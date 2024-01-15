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
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import io.starburst.server.troubleshooting.TroubleshootingTestHelper.Unzipped;
import io.starburst.server.troubleshooting.providers.TroubleshootingProvider;
import io.airlift.bootstrap.Bootstrap;
import io.trino.execution.QueryInfo;
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
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.Executors.newScheduledThreadPool;

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

        TroubleshootingContextManager manager = new Bootstrap(new AbstractModule()
        {
            @Override
            protected void configure()
            {
                bind(FullQueryInfoProvider.class).to(FullQueryInfoProviderTesting.class).in(Scopes.SINGLETON);
                bind(TroubleshootingContextManager.class).in(Scopes.SINGLETON);
                bind(TroubleshootingArchiver.class).in(Scopes.SINGLETON);
                bind(TroubleshootingConfig.class).in(Scopes.SINGLETON);
                bind(ScheduledExecutorService.class).annotatedWith(ForTroubleshooting.class)
                        .toInstance(newScheduledThreadPool(1, daemonThreadsNamed("query-troubleshooting-%s")));

                Multibinder<TroubleshootingProvider> setBinder = newSetBinder(binder(), TroubleshootingProvider.class);
                setBinder.addBinding().to(ThrowingProvider.class).in(Scopes.SINGLETON);
                setBinder.addBinding().toInstance(new HappyPathProvider(onContextStartedCalled, onContextFinishedCalled));
            }
        })
                .initialize()
                .getInstance(TroubleshootingContextManager.class);

        QueryId queryId = new QueryId("123");
        manager.start(queryId);
        manager.finish(queryId);

        Unzipped unzipped = zipInputStreamToMap(manager.getArchive(queryId).get(), tmpDir);
        softly.assertThat(unzipped.zipEntryContents)
                .hasSize(3)
                .hasEntrySatisfying("123/happy", b -> softly.assertThat(new String(b, UTF_8)).isEqualTo("path"))
                .hasEntrySatisfying("123/throwWhileReading.txt", b -> softly.assertThat(b.length).isZero())
                .hasEntrySatisfying("123/largeFile.txt", b -> softly.assertThat(b.length).isEqualTo(1024 * 1024 * 100));

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
