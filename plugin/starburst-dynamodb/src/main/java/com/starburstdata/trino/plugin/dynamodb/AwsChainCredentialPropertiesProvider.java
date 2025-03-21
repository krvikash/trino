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
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

import java.util.Map;

import static com.starburstdata.trino.plugin.dynamodb.DynamoDbConnectorPropertiesConstants.AUTH_SCHEME;
import static com.starburstdata.trino.plugin.dynamodb.DynamoDbConnectorPropertiesConstants.AWS_ACCESS_KEY;
import static com.starburstdata.trino.plugin.dynamodb.DynamoDbConnectorPropertiesConstants.AWS_ROOT_KEYS;
import static com.starburstdata.trino.plugin.dynamodb.DynamoDbConnectorPropertiesConstants.AWS_SECRET_KEY;
import static com.starburstdata.trino.plugin.dynamodb.DynamoDbConnectorPropertiesConstants.AWS_SESSION_TOKEN;
import static com.starburstdata.trino.plugin.dynamodb.DynamoDbConnectorPropertiesConstants.TEMPORARY_CREDENTIALS;
import static java.util.Objects.requireNonNull;

public class AwsChainCredentialPropertiesProvider
        implements CredentialPropertiesProvider
{
    private final AwsCredentialsProvider awsCredentialsProvider;
    private final AwsRolePropertiesProvider awsRolePropertiesProvider;

    @Inject
    public AwsChainCredentialPropertiesProvider(AwsRolePropertiesProvider awsRolePropertiesProvider, AwsCredentialsProvider awsCredentialsProvider)
    {
        this.awsRolePropertiesProvider = requireNonNull(awsRolePropertiesProvider, "awsRolePropertiesProvider is null");
        this.awsCredentialsProvider = requireNonNull(awsCredentialsProvider, "awsCredentialsProvider is null");
    }

    @Override
    public Map<String, Object> getCredentialProperties(ConnectorIdentity identity)
    {
        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
        AwsCredentials credentials = awsCredentialsProvider.resolveCredentials();
        properties.put(AWS_ACCESS_KEY, credentials.accessKeyId());
        properties.put(AWS_SECRET_KEY, credentials.secretAccessKey());
        if (credentials instanceof AwsSessionCredentials awsSessionCredentials) {
            properties.put(AWS_SESSION_TOKEN, awsSessionCredentials.sessionToken());
            properties.put(AUTH_SCHEME, awsRolePropertiesProvider.getAuthScheme(TEMPORARY_CREDENTIALS));
        }
        else {
            properties.put(AUTH_SCHEME, awsRolePropertiesProvider.getAuthScheme(AWS_ROOT_KEYS));
        }

        properties.putAll(awsRolePropertiesProvider.getAwsRoleArnProperties());
        return properties.buildOrThrow();
    }
}
