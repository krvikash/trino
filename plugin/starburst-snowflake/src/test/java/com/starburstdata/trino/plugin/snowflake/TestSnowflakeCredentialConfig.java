/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */

package com.starburstdata.trino.plugin.snowflake;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestSnowflakeCredentialConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(SnowflakeCredentialConfig.class)
                .setPrivateKey(null)
                .setPrivateKeyFile(null)
                .setPrivateKeyPassphrase(null)
                .setConnectionUser(null)
                .setConnectionPassword(null));
    }

    @Test
    public void testExplicitPropertyMappings()
            throws IOException
    {
        Path keyFile = Files.createTempFile(null, null);

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("snowflake.connection-private-key", "key")
                .put("snowflake.connection-private-key-file", keyFile.toString())
                .put("snowflake.connection-private-key.passphrase", "passphrase")
                .put("connection-user", "user")
                .put("connection-password", "password")
                .buildOrThrow();

        SnowflakeCredentialConfig expected = new SnowflakeCredentialConfig();
        expected.setPrivateKey("key")
                .setPrivateKeyFile(keyFile.toString())
                .setPrivateKeyPassphrase("passphrase")
                .setConnectionUser("user")
                .setConnectionPassword("password");

        assertFullMapping(properties, expected);
    }

    @Test
    public void testInvalidSetting()
    {
        SnowflakeCredentialConfig noUsernameConfig = new SnowflakeCredentialConfig();
        assertThatThrownBy(noUsernameConfig::validate)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Connection user is not configured");

        SnowflakeCredentialConfig bothKeyFileOptionsConfig = new SnowflakeCredentialConfig();
        bothKeyFileOptionsConfig.setConnectionUser("username");
        bothKeyFileOptionsConfig.setPrivateKey("key");
        bothKeyFileOptionsConfig.setPrivateKeyFile("key-file");
        assertThatThrownBy(bothKeyFileOptionsConfig::validate)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("snowflake.connection-private-key and snowflake.connection-private-key-file cannot be set simultaneously");

        SnowflakeCredentialConfig keyAndPasswordConfig = new SnowflakeCredentialConfig();
        keyAndPasswordConfig.setConnectionUser("username");
        keyAndPasswordConfig.setConnectionPassword("password");
        keyAndPasswordConfig.setPrivateKey("key");
        assertThatThrownBy(keyAndPasswordConfig::validate)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Either password or private key must be set, but not both");

        SnowflakeCredentialConfig keyFileAndPasswordConfig = new SnowflakeCredentialConfig();
        keyFileAndPasswordConfig.setConnectionUser("username");
        keyFileAndPasswordConfig.setConnectionPassword("password");
        keyFileAndPasswordConfig.setPrivateKeyFile("key");
        assertThatThrownBy(keyFileAndPasswordConfig::validate)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Either password or private key must be set, but not both");

        SnowflakeCredentialConfig passwordAndPassphraseConfig = new SnowflakeCredentialConfig();
        passwordAndPassphraseConfig.setConnectionUser("username");
        passwordAndPassphraseConfig.setConnectionPassword("password");
        passwordAndPassphraseConfig.setPrivateKeyPassphrase("key");
        assertThatThrownBy(passwordAndPassphraseConfig::validate)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("snowflake.connection-private-key.passphrase is set, but snowflake.connection-private-key or snowflake.connection-private-key-file is missing");
    }
}
