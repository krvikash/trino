/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.client.spooling.trinofs;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.starburst.stargate.buffer.data.client.DataApiConfig;
import io.starburst.stargate.buffer.data.client.spooling.SpoolingStorageType;
import io.starburst.stargate.buffer.data.spooling.trinofs.ForTrinoFsSpooling;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.azure.AzureFileSystemFactory;
import io.trino.filesystem.azure.AzureFileSystemModule;
import io.trino.filesystem.gcs.GcsFileSystemFactory;
import io.trino.filesystem.gcs.GcsFileSystemModule;
import io.trino.filesystem.local.LocalFileSystemConfig;
import io.trino.filesystem.local.LocalFileSystemFactory;
import io.trino.filesystem.s3.FileSystemS3;
import io.trino.filesystem.s3.S3FileSystemModule;
import io.trino.spi.security.ConnectorIdentity;
import jakarta.annotation.PreDestroy;

import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static com.google.inject.Scopes.SINGLETON;
import static io.airlift.concurrent.Threads.threadsNamed;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

/**
 * Provisions the single {@link TrinoFileSystem} and backing executor consumed by
 * {@link TrinoFsSpooledChunkReader}. The {@code dataApiName} prefix selects the
 * {@link DataApiConfig} that drives storage-type selection; filesystem-library configs and
 * {@link TrinoFsClientConfig} are namespace-agnostic, so they bind under
 * {@code dataApiName + ".spooling"}. This keeps every spooling-related property under the
 * same {@code .spooling.} namespace produced by {@code BufferExchangeManagerFactory}
 * (see {@code EXCHANGE_SPOOLING_CONFIG_PREFIX}).
 */
public class TrinoFsClientModule
        extends AbstractConfigurationAwareModule
{
    private final String dataApiName;
    private final String spoolingConfigPrefix;

    public TrinoFsClientModule(String dataApiName)
    {
        this.dataApiName = requireNonNull(dataApiName, "dataApiName is null");
        this.spoolingConfigPrefix = dataApiName + ".spooling";
    }

    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(TrinoFsClientConfig.class, spoolingConfigPrefix);
        binder.bind(TrinoFsExecutorLifecycle.class).in(SINGLETON);

        SpoolingStorageType spoolingStorageType = buildConfigObject(DataApiConfig.class, dataApiName).getSpoolingStorageType();
        switch (spoolingStorageType) {
            case LOCAL -> {
                configBinder(binder).bindConfig(LocalFileSystemConfig.class, spoolingConfigPrefix);
                binder.bind(TrinoFileSystemFactory.class).annotatedWith(ForTrinoFsSpooling.class)
                        .to(LocalFileSystemFactory.class).in(SINGLETON);
            }
            case S3 -> {
                install(new S3FileSystemModule(Optional.of(spoolingConfigPrefix)));
                // S3FileSystemModule binds TrinoFileSystemFactory under @FileSystemS3; alias it.
                binder.bind(TrinoFileSystemFactory.class).annotatedWith(ForTrinoFsSpooling.class)
                        .to(Key.get(TrinoFileSystemFactory.class, FileSystemS3.class));
            }
            case GCS -> {
                install(new GcsFileSystemModule(Optional.of(spoolingConfigPrefix)));
                binder.bind(TrinoFileSystemFactory.class).annotatedWith(ForTrinoFsSpooling.class)
                        .to(GcsFileSystemFactory.class).in(SINGLETON);
            }
            case AZURE -> {
                install(new AzureFileSystemModule(Optional.of(spoolingConfigPrefix)));
                binder.bind(TrinoFileSystemFactory.class).annotatedWith(ForTrinoFsSpooling.class)
                        .to(AzureFileSystemFactory.class).in(SINGLETON);
            }
            case NONE -> binder.addError("Cannot use TrinoFileSystem spooling with spooling-storage-type=NONE");
        }
    }

    @Provides
    @Singleton
    @ForTrinoFsSpooling
    static TrinoFileSystem trinoFileSystem(@ForTrinoFsSpooling TrinoFileSystemFactory factory)
    {
        return factory.create(ConnectorIdentity.ofUser("buffer"));
    }

    @Provides
    @Singleton
    @ForTrinoFsSpooling
    static ListeningExecutorService trinoFsExecutor(TrinoFsClientConfig config)
    {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                config.getExecutorThreads(),
                config.getExecutorThreads(),
                60L,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(),
                threadsNamed("trino-fs-client-spooling-%s"));
        executor.allowCoreThreadTimeOut(true);
        return listeningDecorator(executor);
    }

    /**
     * Holder bound as a {@link Singleton} in the Guice graph so Airlift's lifecycle manager
     * invokes {@link #shutdown()} on JVM shutdown. {@code @PreDestroy} on the surrounding
     * {@link AbstractConfigurationAwareModule} would never fire — Guice does not manage Module
     * instances, and Airlift's lifecycle only inspects bound objects.
     */
    static class TrinoFsExecutorLifecycle
    {
        private final ListeningExecutorService executor;

        @Inject
        TrinoFsExecutorLifecycle(@ForTrinoFsSpooling ListeningExecutorService executor)
        {
            this.executor = requireNonNull(executor, "executor is null");
        }

        @PreDestroy
        public void shutdown()
        {
            executor.shutdownNow();
        }
    }
}
