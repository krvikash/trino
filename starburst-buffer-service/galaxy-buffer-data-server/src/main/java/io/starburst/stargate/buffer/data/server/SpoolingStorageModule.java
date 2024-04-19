/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.server;

import com.azure.storage.blob.BlobServiceAsyncClient;
import com.google.inject.Binder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.starburst.stargate.buffer.data.execution.ChunkManagerConfig;
import io.starburst.stargate.buffer.data.spooling.SpoolingStorage;
import io.starburst.stargate.buffer.data.spooling.azure.AzureBlobClientConfig;
import io.starburst.stargate.buffer.data.spooling.azure.AzureBlobSpoolingConfig;
import io.starburst.stargate.buffer.data.spooling.azure.AzureBlobSpoolingStorage;
import io.starburst.stargate.buffer.data.spooling.azure.BlobServiceAsyncClientProvider;
import io.starburst.stargate.buffer.data.spooling.gcs.GcsClientConfig;
import io.starburst.stargate.buffer.data.spooling.local.LocalSpoolingStorage;
import io.starburst.stargate.buffer.data.spooling.s3.S3ClientConfig;
import io.starburst.stargate.buffer.data.spooling.s3.S3ClientProvider;
import io.starburst.stargate.buffer.data.spooling.s3.S3SpoolingStorage;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.net.URI;

import static com.google.inject.Scopes.SINGLETON;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.starburst.stargate.buffer.data.spooling.s3.S3SpoolingStorage.CompatibilityMode.AWS;
import static io.starburst.stargate.buffer.data.spooling.s3.S3SpoolingStorage.CompatibilityMode.GCP;

public class SpoolingStorageModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        URI spoolingBaseDirectory = buildConfigObject(ChunkManagerConfig.class).getSpoolingDirectory();
        String scheme = spoolingBaseDirectory.getScheme();
        if (scheme == null || scheme.equals("file")) {
            binder.bind(SpoolingStorage.class).to(LocalSpoolingStorage.class).in(SINGLETON);
        }
        else if (scheme.equals("s3") || scheme.equals("gs")) {
            configBinder(binder).bindConfig(S3ClientConfig.class);
            configBinder(binder).bindConfig(GcsClientConfig.class);
            binder.bind(S3AsyncClient.class).toProvider(S3ClientProvider.class).in(SINGLETON);
            binder.bind(S3SpoolingStorage.CompatibilityMode.class).toInstance(scheme.equals("s3") ? AWS : GCP);
            binder.bind(SpoolingStorage.class).to(S3SpoolingStorage.class).in(SINGLETON);
        }
        else if (scheme.equals("abfs")) {
            configBinder(binder).bindConfig(AzureBlobClientConfig.class);
            configBinder(binder).bindConfig(AzureBlobSpoolingConfig.class);
            binder.bind(BlobServiceAsyncClient.class).toProvider(BlobServiceAsyncClientProvider.class).in(SINGLETON);
            binder.bind(SpoolingStorage.class).to(AzureBlobSpoolingStorage.class).in(SINGLETON);
        }
        else {
            binder.addError("Scheme %s is not supported as buffer spooling storage".formatted(scheme));
        }
    }
}
