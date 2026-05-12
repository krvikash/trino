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
import io.airlift.configuration.ConfigDescription;
import io.starburst.stargate.buffer.data.client.spooling.SpoolingClientDriver;
import io.starburst.stargate.buffer.data.client.spooling.SpoolingStorageType;
import jakarta.validation.constraints.NotNull;

import static io.starburst.stargate.buffer.data.client.spooling.SpoolingClientDriver.NATIVE;
import static io.starburst.stargate.buffer.data.client.spooling.SpoolingStorageType.NONE;

public class DataApiConfig
{
    private boolean dataIntegrityVerificationEnabled = true;
    private SpoolingStorageType spoolingStorageType = NONE;
    private SpoolingClientDriver spoolingClientDriver = NATIVE;

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

    @NotNull
    public SpoolingClientDriver getSpoolingClientDriver()
    {
        return spoolingClientDriver;
    }

    @Config("spooling-client-driver")
    @ConfigDescription("Selects which client-side spooled-chunk reader to use. NATIVE (default) picks the per-scheme native readers selected by spooling-storage-type. TRINO_FS reads through the generic TrinoFileSystem abstraction.")
    public DataApiConfig setSpoolingClientDriver(SpoolingClientDriver spoolingClientDriver)
    {
        this.spoolingClientDriver = spoolingClientDriver;
        return this;
    }
}
