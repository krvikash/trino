/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.spooling.trinofs;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.starburst.stargate.buffer.data.execution.SpoolingDirectoryConfig;
import io.starburst.stargate.buffer.data.spooling.SpoolingStorage;
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

import java.net.URI;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static com.google.inject.Scopes.SINGLETON;
import static io.airlift.concurrent.Threads.threadsNamed;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

/**
 * Wires the TrinoFileSystem-backed spooling backend.
 *
 * <p>Reads the URI scheme of {@code spooling.directory}, installs the matching
 * {@code trino-filesystem-*} module, and exposes a singleton {@link TrinoFileSystem}
 * (annotated {@link ForTrinoFsSpooling}) plus two {@link ListeningExecutorService}
 * pools — one for read/write traffic ({@link ForTrinoFsSpooling}) and a smaller one
 * for delete traffic ({@link ForTrinoFsSpoolingDelete}).
 *
 * <p>Both executor pools are shut down with {@code shutdownNow()} so that any in-flight
 * blocking call receives an interrupt; cancellation of futures returned by
 * {@link TrinoFsSpoolingStorage} relies on the same mechanism.
 */
public class TrinoFsSpoolingStorageModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(TrinoFsSpoolingConfig.class);

        SpoolingDirectoryConfig spoolingDirectoryConfig = buildConfigObject(SpoolingDirectoryConfig.class);
        URI spoolingDirectory = spoolingDirectoryConfig.getSpoolingDirectory();
        String scheme = spoolingDirectory.getScheme();
        if (scheme == null || scheme.equals("file") || scheme.equals("local")) {
            if (!spoolingDirectoryConfig.isAllowLocalSpooling()) {
                binder.addError("Local filesystem spooling is not supported (storage-driver=TRINO_FS, scheme=%s).".formatted(scheme));
                // Skip the LocalFileSystemConfig / factory binds; addError is deferred until
                // injector creation and a downstream bindConfig of a missing property would emit
                // a second, confusing error on top of the real one.
                return;
            }
            // LocalFileSystemFactory is documented in trino-filesystem as "for testing"; it
            // caches a single LocalFileSystem instance and ignores ConnectorIdentity. Use behind
            // testing.allow-local-spooling=true only.
            configBinder(binder).bindConfig(LocalFileSystemConfig.class);
            binder.bind(TrinoFileSystemFactory.class).to(LocalFileSystemFactory.class).in(SINGLETON);
        }
        else if (scheme.equals("s3") || scheme.equals("s3a") || scheme.equals("s3n")) {
            install(new S3FileSystemModule());
            // S3FileSystemModule binds TrinoFileSystemFactory under @FileSystemS3.
            // Link the unannotated key to the annotated one so we share the singleton.
            binder.bind(TrinoFileSystemFactory.class).to(Key.get(TrinoFileSystemFactory.class, FileSystemS3.class));
        }
        else if (scheme.equals("gs")) {
            install(new GcsFileSystemModule());
            binder.bind(TrinoFileSystemFactory.class).to(GcsFileSystemFactory.class).in(SINGLETON);
        }
        else if (scheme.equals("abfs") || scheme.equals("abfss") || scheme.equals("wasb") || scheme.equals("wasbs")) {
            install(new AzureFileSystemModule());
            binder.bind(TrinoFileSystemFactory.class).to(AzureFileSystemFactory.class).in(SINGLETON);
        }
        else {
            binder.addError("Scheme %s is not supported by TRINO_FS spooling driver".formatted(scheme));
        }

        binder.bind(SpoolingStorage.class).to(TrinoFsSpoolingStorage.class).in(SINGLETON);
        binder.bind(TrinoFsExecutorLifecycle.class).in(SINGLETON);
    }

    @Provides
    @Singleton
    @ForTrinoFsSpooling
    static TrinoFileSystem trinoFileSystem(TrinoFileSystemFactory factory)
    {
        return factory.create(ConnectorIdentity.ofUser("buffer"));
    }

    @Provides
    @Singleton
    @ForTrinoFsSpooling
    static ListeningExecutorService trinoFsExecutor(TrinoFsSpoolingConfig config)
    {
        return newPool(config.getExecutorThreads(), "trino-fs-spooling-%s");
    }

    @Provides
    @Singleton
    @ForTrinoFsSpoolingDelete
    static ListeningExecutorService trinoFsDeleteExecutor(TrinoFsSpoolingConfig config)
    {
        return newPool(config.getDeleteExecutorThreads(), "trino-fs-spooling-delete-%s");
    }

    private static ListeningExecutorService newPool(int threads, String nameFormat)
    {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                threads,
                threads,
                60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(),
                threadsNamed(nameFormat));
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
        private final ListeningExecutorService readWriteExecutor;
        private final ListeningExecutorService deleteExecutor;

        @Inject
        TrinoFsExecutorLifecycle(
                @ForTrinoFsSpooling ListeningExecutorService readWriteExecutor,
                @ForTrinoFsSpoolingDelete ListeningExecutorService deleteExecutor)
        {
            this.readWriteExecutor = requireNonNull(readWriteExecutor, "readWriteExecutor is null");
            this.deleteExecutor = requireNonNull(deleteExecutor, "deleteExecutor is null");
        }

        @PreDestroy
        public void shutdown()
        {
            // shutdownNow() so in-flight blocking TrinoFileSystem calls receive an interrupt,
            // matching the cancellation contract documented on TrinoFsSpoolingStorage.
            readWriteExecutor.shutdownNow();
            deleteExecutor.shutdownNow();
        }
    }
}
