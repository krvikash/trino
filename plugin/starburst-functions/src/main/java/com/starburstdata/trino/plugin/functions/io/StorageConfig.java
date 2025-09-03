/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.functions.io;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.configuration.validation.FileExists;
import jakarta.validation.constraints.AssertTrue;

import java.io.File;
import java.util.Optional;

public class StorageConfig
{
    private Optional<String> credentialsKey = Optional.empty();
    private Optional<File> credentialsFile = Optional.empty();
    private boolean useRowSemantics;

    public Optional<String> getCredentialsKey()
    {
        return credentialsKey;
    }

    @Config("io.credentials-key")
    @ConfigDescription("The base64 encoded credentials key")
    @ConfigSecuritySensitive
    public StorageConfig setCredentialsKey(String credentialsKey)
    {
        this.credentialsKey = Optional.ofNullable(credentialsKey);
        return this;
    }

    public Optional<@FileExists File> getCredentialsFile()
    {
        return credentialsFile;
    }

    @Config("io.credentials-file")
    @ConfigDescription("Path to the file containing the credentials")
    public StorageConfig setCredentialsFile(File credentialsFile)
    {
        this.credentialsFile = Optional.ofNullable(credentialsFile);
        return this;
    }

    public boolean isUseRowSemantics()
    {
        return useRowSemantics;
    }

    @Config("io.unload.use-row-semantics")
    @ConfigDescription("Forces unload function to use row semantics. PARTITION BY and ORDER BY clauses are not supported")
    public StorageConfig setUseRowSemantics(boolean useRowSemantics)
    {
        this.useRowSemantics = useRowSemantics;
        return this;
    }

    @AssertTrue(message = "Exactly one of 'io.credentials-key' or 'io.credentials-file' must be specified")
    public boolean isCredentialsConfigurationValid()
    {
        // only one of them (at most) should be present
        return credentialsKey.isEmpty() || credentialsFile.isEmpty();
    }
}
