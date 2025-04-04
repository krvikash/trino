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
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class DynamoDbCredentialPropertiesProvider
        implements CredentialPropertiesProvider
{
    private static final String AUTH_SCHEME = "AuthScheme";
    private static final String AWS_ACCESS_KEY = "AWS Access Key";
    private static final String AWS_SECRET_KEY = "AWS Secret Key";
    private static final String AWS_SESSION_TOKEN = "AWSSessionToken";
    private static final String AWS_ROLE_ARN = "AWS Role Arn";
    private static final String CREDENTIALS_LOCATION = "CredentialsLocation";

    private static final String TEMPORARY_CREDENTIALS = "TemporaryCredentials";
    private static final String AWS_EC2_ROLES = "AwsEC2Roles";
    private static final String AWS_IAM_ROLES = "AwsIAMRoles";
    private static final String AWS_ROOT_KEYS = "AwsRootKeys";

    private final Optional<String> awsAccessKey;
    private final Optional<String> awsSecretKey;
    private final Optional<String> awsRoleArn;
    private final String awsRoleCredentialsLocation;
    private final Optional<AwsCredentialsProvider> awsChainCredentialsProvider;

    @Inject
    public DynamoDbCredentialPropertiesProvider(DynamoDbConfig dynamoDbConfig, Optional<AwsCredentialsProvider> awsChainCredentialsProvider)
    {
        awsAccessKey = requireNonNull(dynamoDbConfig.getAwsAccessKey(), "accessKey is null");
        awsSecretKey = requireNonNull(dynamoDbConfig.getAwsSecretKey(), "secretKey is null");
        awsRoleArn = requireNonNull(dynamoDbConfig.getAwsRoleArn(), "roleArn is null");
        awsRoleCredentialsLocation = requireNonNull(dynamoDbConfig.getAwsRoleCredentialsLocation(), "roleCredentialsLocation is null");

        this.awsChainCredentialsProvider = requireNonNull(awsChainCredentialsProvider, "awsChainCredentialsProvider is null");
    }

    @Override
    public Map<String, Object> getCredentialProperties(ConnectorIdentity identity)
    {
        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
        // Both of these settings are validated in DynamoDbConfig
        if (awsAccessKey.isPresent() && awsSecretKey.isPresent()) {
            properties.put(AWS_ACCESS_KEY, awsAccessKey.get());
            properties.put(AWS_SECRET_KEY, awsSecretKey.get());
            properties.put(AUTH_SCHEME, getAuthScheme(awsRoleArn.isPresent()));
        }
        else if (awsChainCredentialsProvider.isPresent()) {
            AwsCredentials credentials = awsChainCredentialsProvider.get().resolveCredentials();
            properties.put(AWS_ACCESS_KEY, credentials.accessKeyId());
            properties.put(AWS_SECRET_KEY, credentials.secretAccessKey());
            if (credentials instanceof AwsSessionCredentials awsSessionCredentials) {
                properties.put(AUTH_SCHEME, TEMPORARY_CREDENTIALS);
                properties.put(AWS_SESSION_TOKEN, awsSessionCredentials.sessionToken());
            }
            else {
                properties.put(AUTH_SCHEME, getAuthScheme(awsRoleArn.isPresent()));
            }
        }
        else {
            // If they are not set, set auth scheme to EC2 roles so driver does not throw an error
            properties.put(AUTH_SCHEME, AWS_EC2_ROLES);
        }

        awsRoleArn.ifPresent(role ->
        {
            properties.put(AWS_ROLE_ARN, role);
            properties.put(CREDENTIALS_LOCATION, awsRoleCredentialsLocation);
        });
        return properties.buildOrThrow();
    }

    private String getAuthScheme(boolean isAwsRoleArnDefined)
    {
        // AwsRootKeys should be overridden to AwsIAMRoles, if aws-role-arn is defined
        return isAwsRoleArnDefined ? AWS_IAM_ROLES : AWS_ROOT_KEYS;
    }
}
