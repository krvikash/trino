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

import static com.google.common.base.Preconditions.checkState;
import static com.starburstdata.trino.plugin.dynamodb.DynamoDbConnectorPropertiesConstants.AUTH_SCHEME;
import static com.starburstdata.trino.plugin.dynamodb.DynamoDbConnectorPropertiesConstants.AWS_ACCESS_KEY;
import static com.starburstdata.trino.plugin.dynamodb.DynamoDbConnectorPropertiesConstants.AWS_ROOT_KEYS;
import static com.starburstdata.trino.plugin.dynamodb.DynamoDbConnectorPropertiesConstants.AWS_SECRET_KEY;
import static java.util.Objects.requireNonNull;

public class ConfigCredentialPropertiesProvider
        implements CredentialPropertiesProvider
{
    private final String awsAccessKey;
    private final String awsSecretKey;
    private final AwsRolePropertiesProvider awsRolePropertiesProvider;

    @Inject
    public ConfigCredentialPropertiesProvider(DynamoDbConfig dynamoDbConfig, AwsRolePropertiesProvider awsRolePropertiesProvider)
    {
        requireNonNull(dynamoDbConfig, "dynamoDbConfig is null");
        requireNonNull(dynamoDbConfig.getAwsAccessKey(), "awsAccessKey is null");
        requireNonNull(dynamoDbConfig.getAwsSecretKey(), "awsSecretKey is null");
        checkState(dynamoDbConfig.getAwsAccessKey().isPresent(), "dynamodb.aws-access-key is empty");
        checkState(dynamoDbConfig.getAwsSecretKey().isPresent(), "dynamodb.aws-secret-key is empty");

        awsAccessKey = dynamoDbConfig.getAwsAccessKey().get();
        awsSecretKey = dynamoDbConfig.getAwsSecretKey().get();
        this.awsRolePropertiesProvider = requireNonNull(awsRolePropertiesProvider, "awsRolePropertiesProvider is null");
    }

    @Override
    public Map<String, Object> getCredentialProperties(ConnectorIdentity identity)
    {
        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();

        properties.put(AWS_ACCESS_KEY, awsAccessKey);
        properties.put(AWS_SECRET_KEY, awsSecretKey);
        properties.put(AUTH_SCHEME, awsRolePropertiesProvider.getAuthScheme(AWS_ROOT_KEYS));

        properties.putAll(awsRolePropertiesProvider.getAwsRoleArnProperties());
        return properties.buildOrThrow();
    }
}
