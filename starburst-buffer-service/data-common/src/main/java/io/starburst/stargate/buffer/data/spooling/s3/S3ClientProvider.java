/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.spooling.s3;

import com.google.inject.Inject;
import com.google.inject.Provider;
import software.amazon.awssdk.services.s3.S3AsyncClient;

public class S3ClientProvider
        implements Provider<S3AsyncClient>
{
    private final S3ClientConfig s3ClientConfig;

    @Inject
    public S3ClientProvider(S3ClientConfig s3ClientConfig)
    {
        this.s3ClientConfig = s3ClientConfig;
    }

    @Override
    public S3AsyncClient get()
    {
        return S3Utils.createS3Client(s3ClientConfig);
    }
}
