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

import com.google.common.collect.ImmutableMap;
import io.airlift.bootstrap.Bootstrap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestSpoolingStorageModule
{
    @Test
    public void testLocalDirectorySucceedsWithAllowLocalSpooling(@TempDir Path tempDir)
    {
        createBootstrap(Map.of(
                "spooling.directory", tempDir.toString(),
                "testing.allow-local-spooling", "true"));
    }

    @Test
    public void testFailsForLocalDirectoryWithoutAllowFlag(@TempDir Path tempDir)
    {
        assertThatThrownBy(() -> createBootstrap(Map.of(
                "spooling.directory", tempDir.toString())))
                .hasMessageContaining("Local filesystem spooling is not supported");
    }

    @Test
    public void testFailsForFileSchemeWithoutAllowFlag(@TempDir Path tempDir)
    {
        assertThatThrownBy(() -> createBootstrap(Map.of(
                "spooling.directory", "file://" + tempDir)))
                .hasMessageContaining("Local filesystem spooling is not supported");
    }

    @Test
    public void testS3DirectorySucceedsWithS3Properties()
    {
        createBootstrap(Map.of(
                "spooling.directory", "s3://spooling-bucket",
                "spooling.s3.region", "us-east-1"));
    }

    @Test
    public void testGcsDirectorySucceedsWithGcsProperties()
    {
        createBootstrap(Map.of(
                "spooling.directory", "gs://spooling-bucket",
                "spooling.gcs.json-key", "/path/to/key.json"));
    }

    @Test
    public void testAzureDirectorySucceedsWithAzureProperties()
    {
        createBootstrap(Map.of(
                "spooling.directory", "abfs://container@spooling",
                "spooling.azure.connection-string", "DefaultEndpointsProtocol=https;AccountName=test"));
    }

    @Test
    public void testFailsForLocalDirectoryWithS3Properties(@TempDir Path tempDir)
    {
        assertThatThrownBy(() -> createBootstrap(Map.of(
                "spooling.directory", tempDir.toString(),
                "spooling.s3.region", "us-east-1")))
                .hasMessageContaining("Local filesystem spooling is not supported");
    }

    @Test
    public void testFailsForFileSchemeDirectoryWithS3Properties(@TempDir Path tempDir)
    {
        assertThatThrownBy(() -> createBootstrap(Map.of(
                "spooling.directory", "file://" + tempDir,
                "spooling.s3.region", "us-east-1")))
                .hasMessageContaining("Local filesystem spooling is not supported");
    }

    @Test
    public void testFailsForLocalDirectoryWithConfigPrefix(@TempDir Path tempDir)
    {
        assertThatThrownBy(() -> createBootstrap(
                Optional.of("buffer"),
                Map.of("buffer.spooling.directory", tempDir.toString())))
                .hasMessageContaining("Local filesystem spooling is not supported");
    }

    private static void createBootstrap(Map<String, String> properties)
    {
        createBootstrap(Optional.empty(), properties);
    }

    private static void createBootstrap(Optional<String> configPrefix, Map<String, String> properties)
    {
        Bootstrap app = new Bootstrap(new SpoolingStorageModule(configPrefix, true));
        app.quiet()
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(ImmutableMap.copyOf(properties))
                .initialize();
    }
}
