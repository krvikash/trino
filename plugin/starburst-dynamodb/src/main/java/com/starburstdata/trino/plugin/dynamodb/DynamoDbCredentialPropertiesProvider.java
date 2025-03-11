/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.dynamodb;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.trino.plugin.jdbc.credential.CredentialPropertiesProvider;
import io.trino.spi.security.ConnectorIdentity;

import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class DynamoDbCredentialPropertiesProvider
        implements CredentialPropertiesProvider
{
    private final Optional<String> awsAccessKey;
    private final Optional<String> awsSecretKey;
    private final Optional<String> awsRoleArn;
    private final String awsRoleCredentialsLocation;

    @Inject
    public DynamoDbCredentialPropertiesProvider(DynamoDbConfig dynamoDbConfig)
    {
        awsAccessKey = requireNonNull(dynamoDbConfig.getAwsAccessKey(), "accessKey is null");
        awsSecretKey = requireNonNull(dynamoDbConfig.getAwsSecretKey(), "secretKey is null");
        awsRoleArn = requireNonNull(dynamoDbConfig.getAwsRoleArn(), "roleArn is null");
        awsRoleCredentialsLocation = requireNonNull(dynamoDbConfig.getAwsRoleCredentialsLocation(), "roleCredentialsLocation is null");
    }

    @Override
    public Map<String, Object> getCredentialProperties(ConnectorIdentity identity)
    {
        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
        // Both of these settings are validated in DynamoDbConfig
        if (awsAccessKey.isPresent() && awsSecretKey.isPresent()) {
            properties.put("AWS Access Key", awsAccessKey.get());
            properties.put("AWS Secret Key", awsSecretKey.get());
        }
        else {
            // If they are not set, set auth scheme to EC2 roles so driver does not throw an error
            properties.put("Auth Scheme", "AwsEC2Roles");
        }

        awsRoleArn.ifPresent(role ->
        {
            properties.put("AuthScheme", "AwsIAMRoles");
            properties.put("AWS Role Arn", role);
            properties.put("CredentialsLocation", awsRoleCredentialsLocation);
        });
        return properties.buildOrThrow();
    }
}
