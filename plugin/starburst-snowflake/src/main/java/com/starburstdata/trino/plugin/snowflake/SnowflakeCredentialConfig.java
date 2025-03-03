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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.configuration.validation.FileExists;
import io.trino.plugin.jdbc.credential.CredentialConfig;
import jakarta.annotation.PostConstruct;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;

public class SnowflakeCredentialConfig
        extends CredentialConfig
{
    private Optional<String> privateKey = Optional.empty();
    private Optional<String> privateKeyFile = Optional.empty();
    private Optional<String> privateKeyPassphrase = Optional.empty();

    public Optional<String> getPrivateKey()
    {
        return privateKey;
    }

    @Config("snowflake.connection-private-key")
    @ConfigDescription("The base64 encoded private key for key-pair authentication")
    @ConfigSecuritySensitive
    public SnowflakeCredentialConfig setPrivateKey(String privateKey)
    {
        this.privateKey = Optional.ofNullable(privateKey);
        return this;
    }

    public Optional<@FileExists String> getPrivateKeyFile()
    {
        return privateKeyFile;
    }

    @Config("snowflake.connection-private-key-file")
    @ConfigDescription("The file path of the private key for key-pair authentication")
    public SnowflakeCredentialConfig setPrivateKeyFile(String privateKeyFile)
    {
        this.privateKeyFile = Optional.ofNullable(privateKeyFile);
        return this;
    }

    public Optional<String> getPrivateKeyPassphrase()
    {
        return privateKeyPassphrase;
    }

    @Config("snowflake.connection-private-key.passphrase")
    @ConfigDescription("The passphrase to the key-pair authentication private key")
    @ConfigSecuritySensitive
    public SnowflakeCredentialConfig setPrivateKeyPassphrase(String privateKeyPassphrase)
    {
        this.privateKeyPassphrase = Optional.ofNullable(privateKeyPassphrase);
        return this;
    }

    @PostConstruct
    public void validate()
    {
        checkState(getConnectionUser().isPresent(), "Connection user is not configured");
        checkState(
                (getPrivateKey().isPresent() || getPrivateKeyFile().isPresent()) != getConnectionPassword().isPresent(),
                "Either password or private key must be set, but not both");
        if (getConnectionPassword().isEmpty()) {
            checkState(
                    getPrivateKey().isPresent() != getPrivateKeyFile().isPresent(),
                    "snowflake.connection-private-key and snowflake.connection-private-key-file cannot be set simultaneously");
        }
        if (getPrivateKeyPassphrase().isPresent()) {
            checkState(
                    getPrivateKey().isPresent() || getPrivateKeyFile().isPresent(),
                    "snowflake.connection-private-key.passphrase is set, but snowflake.connection-private-key or snowflake.connection-private-key-file is missing");
        }
    }
}
