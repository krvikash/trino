/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.client;

import io.airlift.configuration.Config;
import io.starburst.stargate.buffer.data.client.spooling.SpoolingStorageType;

import javax.validation.constraints.NotNull;

public class DataApiConfig
{
    private boolean dataIntegrityVerificationEnabled = true;
    private SpoolingStorageType spoolingStorageType = SpoolingStorageType.NONE;

    @Config("data-integrity-verification-enabled")
    public DataApiConfig setDataIntegrityVerificationEnabled(boolean dataIntegrityVerificationEnabled)
    {
        this.dataIntegrityVerificationEnabled = dataIntegrityVerificationEnabled;
        return this;
    }

    public boolean isDataIntegrityVerificationEnabled()
    {
        return dataIntegrityVerificationEnabled;
    }

    @NotNull
    public SpoolingStorageType getSpoolingStorageType()
    {
        return spoolingStorageType;
    }

    @Config("spooling-storage-type")
    public DataApiConfig setSpoolingStorageType(SpoolingStorageType spoolingStorageType)
    {
        this.spoolingStorageType = spoolingStorageType;
        return this;
    }
}
