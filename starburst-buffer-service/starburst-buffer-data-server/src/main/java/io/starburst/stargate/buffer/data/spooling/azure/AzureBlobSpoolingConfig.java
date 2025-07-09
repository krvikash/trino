/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.spooling.azure;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.DataSize;

import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class AzureBlobSpoolingConfig
{
    private DataSize uploadBlockSize = DataSize.of(4, MEGABYTE);
    private int uploadMaxConcurrency = 4;

    @Config("spooling.azure.upload-block-size")
    @ConfigDescription("The block size of multi-part upload for spooling. Default is 4MB")
    public AzureBlobSpoolingConfig setUploadBlockSize(DataSize uploadBlockSize)
    {
        this.uploadBlockSize = uploadBlockSize;
        return this;
    }

    public DataSize getUploadBlockSize()
    {
        return uploadBlockSize;
    }

    @Config("spooling.azure.upload-max-concurrency")
    @ConfigDescription("The maximum spooling concurrency per chunk upload. Default is 4.")
    public AzureBlobSpoolingConfig setUploadMaxConcurrency(int uploadMaxConcurrency)
    {
        this.uploadMaxConcurrency = uploadMaxConcurrency;
        return this;
    }

    public int getUploadMaxConcurrency()
    {
        return uploadMaxConcurrency;
    }
}
