/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.io;

import com.google.common.collect.ImmutableMap;
import com.google.inject.ConfigurationException;
import io.airlift.configuration.ConfigurationFactory;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestStorageConfig
{
    @Test
    void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(StorageConfig.class)
                .setCredentialsKey(null)
                .setCredentialsFile(null));
    }

    @Test
    void testExplicitPropertyMappingsCredentialsKey()
    {
        Map<String, String> properties = ImmutableMap.of("io.credentials-key", "key");

        ConfigurationFactory configurationFactory = new ConfigurationFactory(properties);
        StorageConfig config = configurationFactory.build(StorageConfig.class);

        assertThat(config.getCredentialsKey()).isEqualTo(Optional.of("key"));
        assertThat(config.getCredentialsFile()).isEqualTo(Optional.empty());
    }

    @Test
    void testExplicitPropertyMappingsCredentialsFile()
            throws Exception
    {
        Path file = Files.createTempFile("config", ".json");

        Map<String, String> properties = ImmutableMap.of("io.credentials-file", file.toString());

        ConfigurationFactory configurationFactory = new ConfigurationFactory(properties);
        StorageConfig config = configurationFactory.build(StorageConfig.class);

        assertThat(config.getCredentialsKey()).isEqualTo(Optional.empty());
        assertThat(config.getCredentialsFile()).isEqualTo(Optional.of(file.toFile()));
    }

    @Test
    void testExplicitPropertyMappingsValidation()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("io.credentials-key", "key")
                .put("io.credentials-file", "file")
                .buildOrThrow();

        ConfigurationFactory configurationFactory = new ConfigurationFactory(properties);
        assertThatThrownBy(() -> configurationFactory.build(StorageConfig.class))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("Exactly one of 'io.credentials-key' or 'io.credentials-file' must be specified");
    }
}
