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

import com.google.cloud.storage.StorageRetryStrategy;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.configuration.validation.FileExists;

import java.util.Optional;

public class GcsClientConfig
{
    private Optional<String> gcsJsonKeyFilePath = Optional.empty();
    private Optional<String> gcsJsonKey = Optional.empty();

    private Optional<String> gcsEndpoint = Optional.empty();
    private GcsRetryMode gcsRetryMode = GcsRetryMode.DEFAULT;
    private int threadCount = 50;

    public Optional<@FileExists String> getGcsJsonKeyFilePath()
    {
        return gcsJsonKeyFilePath;
    }

    @Config("spooling.gcs.json-key-file-path")
    @ConfigDescription("Path to the JSON file that contains your Google Cloud Platform service account key. Not to be set together with `exchange.gcs.json-key` or `spooling.gcs.client-id`")
    public GcsClientConfig setGcsJsonKeyFilePath(String gcsJsonKeyFilePath)
    {
        this.gcsJsonKeyFilePath = Optional.ofNullable(gcsJsonKeyFilePath);
        return this;
    }

    public Optional<String> getGcsJsonKey()
    {
        return gcsJsonKey;
    }

    @Config("spooling.gcs.json-key")
    @ConfigDescription("Your Google Cloud Platform service account key in JSON format. Not to be set together with `exchange.gcs.json-key-file-path` or `spooling.gcs.client-id`")
    @ConfigSecuritySensitive
    public GcsClientConfig setGcsJsonKey(String gcsJsonKey)
    {
        this.gcsJsonKey = Optional.ofNullable(gcsJsonKey);
        return this;
    }

    public Optional<String> getGcsEndpoint()
    {
        return gcsEndpoint;
    }

    @Config("spooling.gcs.endpoint")
    public GcsClientConfig setGcsEndpoint(String gcsEndpoint)
    {
        this.gcsEndpoint = Optional.ofNullable(gcsEndpoint);
        return this;
    }

    public StorageRetryStrategy getStorageRetryStrategy()
    {
        switch (gcsRetryMode) {
            case DEFAULT -> {
                return StorageRetryStrategy.getDefaultStorageRetryStrategy();
            }
            case UNIFORM -> {
                return StorageRetryStrategy.getUniformStorageRetryStrategy();
            }
            default -> throw new IllegalArgumentException("Unexpected gcsRetryStrategy %s".formatted(gcsRetryMode));
        }
    }

    public GcsRetryMode getGcsRetryMode()
    {
        return gcsRetryMode;
    }

    @Config("spooling.gcs.retry-mode")
    @ConfigDescription("GCS retry mode; either DEFAULT or UNIFORM.")
    public GcsClientConfig setGcsRetryMode(GcsRetryMode retryMode)
    {
        this.gcsRetryMode = retryMode;
        return this;
    }

    public int getThreadCount()
    {
        return threadCount;
    }

    @Config("spooling.gcs.thread-count")
    public GcsClientConfig setThreadCount(int threadCount)
    {
        this.threadCount = threadCount;
        return this;
    }
}
