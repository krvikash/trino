/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.snowflake.jdbc;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.starburstdata.trino.plugin.snowflake.SnowflakeCredentialConfig;
import io.trino.plugin.jdbc.credential.CredentialPropertiesProvider;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.spi.security.ConnectorIdentity;

import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class SnowflakeCredentialPropertiesProvider
        implements CredentialPropertiesProvider
{
    private final CredentialProvider provider;
    private final SnowflakeCredentialConfig snowflakeCredentialConfig;

    @Inject
    public SnowflakeCredentialPropertiesProvider(CredentialProvider provider, SnowflakeCredentialConfig snowflakeCredentialConfig)
    {
        this.provider = requireNonNull(provider, "provider is null");
        this.snowflakeCredentialConfig = requireNonNull(snowflakeCredentialConfig, "snowflakeCredentialConfig is null");
    }

    @Override
    public Map<String, Object> getCredentialProperties(ConnectorIdentity identity)
    {
        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
        provider.getConnectionUser(Optional.of(identity)).ifPresent(user -> properties.put("user", user));
        Optional<String> connectionPassword = provider.getConnectionPassword(Optional.of(identity));
        if (connectionPassword.isPresent()) {
            verify(
                    snowflakeCredentialConfig.getPrivateKeyFile().isEmpty() && snowflakeCredentialConfig.getPrivateKey().isEmpty(),
                    "Cannot use both password private key authentication");
            verify(snowflakeCredentialConfig.getPrivateKeyPassphrase().isEmpty(), "Cannot use private key passphrase with password authentication");
        }
        else {
            verify(
                    snowflakeCredentialConfig.getPrivateKeyFile().isPresent() != snowflakeCredentialConfig.getPrivateKey().isPresent(),
                    "Either private key or private key file must be set if password is not set");
        }
        connectionPassword.ifPresent(password -> properties.put("password", password));
        snowflakeCredentialConfig.getPrivateKey().ifPresent(key -> properties.put("private_key_base64", key));
        snowflakeCredentialConfig.getPrivateKeyFile().ifPresent(keyFile -> properties.put("private_key_file", keyFile));
        snowflakeCredentialConfig.getPrivateKeyPassphrase().ifPresent(passphrase -> properties.put("private_key_pwd", passphrase));
        return properties.buildOrThrow();
    }
}
