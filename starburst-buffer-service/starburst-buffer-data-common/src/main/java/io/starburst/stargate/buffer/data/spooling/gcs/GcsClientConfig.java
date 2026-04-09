/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.spooling.gcs;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.configuration.validation.FileExists;
import jakarta.annotation.Nullable;
import jakarta.validation.constraints.AssertTrue;

import java.util.Optional;

public class GcsClientConfig
{
    private Optional<String> gcsJsonKey = Optional.empty();
    private Optional<String> gcsJsonKeyFilePath = Optional.empty();
    private int deleteExecutorThreadCount = 50;

    public Optional<String> getGcsJsonKey()
    {
        return gcsJsonKey;
    }

    @Config("spooling.gcs.json-key")
    @ConfigDescription("Your GCP service account key in JSON format")
    @ConfigSecuritySensitive
    public GcsClientConfig setGcsJsonKey(String gcsJsonKey)
    {
        this.gcsJsonKey = Optional.ofNullable(gcsJsonKey);
        return this;
    }

    public Optional<String> getGcsJsonKeyFilePath()
    {
        return gcsJsonKeyFilePath;
    }

    @Nullable
    @FileExists
    public String getGcsJsonKeyFilePathValidated()
    {
        return gcsJsonKeyFilePath.orElse(null);
    }

    @Config("spooling.gcs.json-key-file-path")
    @ConfigDescription("Path to a JSON key file used to access Google Cloud Storage")
    public GcsClientConfig setGcsJsonKeyFilePath(String gcsJsonKeyFilePath)
    {
        this.gcsJsonKeyFilePath = Optional.ofNullable(gcsJsonKeyFilePath);
        return this;
    }

    public int getDeleteExecutorThreadCount()
    {
        return deleteExecutorThreadCount;
    }

    @Config("spooling.gcs.delete-executor-thread-count")
    public GcsClientConfig setDeleteExecutorThreadCount(int deleteExecutorThreadCount)
    {
        this.deleteExecutorThreadCount = deleteExecutorThreadCount;
        return this;
    }

    @AssertTrue(message = "spooling.gcs.json-key and spooling.gcs.json-key-file-path are mutually exclusive")
    public boolean isJsonKeyConfigValid()
    {
        return !(gcsJsonKey.isPresent() && gcsJsonKeyFilePath.isPresent());
    }
}
