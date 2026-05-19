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

import java.util.Map;
import java.util.Optional;

import static com.starburstdata.trino.plugin.dynamodb.DynamoDbConnectorPropertiesConstants.AWS_IAM_ROLES;
import static com.starburstdata.trino.plugin.dynamodb.DynamoDbConnectorPropertiesConstants.AWS_ROLE_ARN;
import static com.starburstdata.trino.plugin.dynamodb.DynamoDbConnectorPropertiesConstants.AWS_ROOT_KEYS;
import static com.starburstdata.trino.plugin.dynamodb.DynamoDbConnectorPropertiesConstants.CREDENTIALS_LOCATION;
import static java.util.Objects.requireNonNull;

public class AwsRolePropertiesProvider
{
    private final Optional<String> awsRoleArn;
    private final String awsRoleCredentialsLocation;

    @Inject
    public AwsRolePropertiesProvider(DynamoDbConfig dynamoDbConfig)
    {
        requireNonNull(dynamoDbConfig, "dynamoDbConfig is null");
        awsRoleArn = requireNonNull(dynamoDbConfig.getAwsRoleArn(), "awsRoleArn is null");
        awsRoleCredentialsLocation = requireNonNull(dynamoDbConfig.getAwsRoleCredentialsLocation(), "awsRoleCredentialsLocation is null");
    }

    public String getAuthScheme(String authScheme)
    {
        // AwsRootKeys should be overridden to AwsIAMRoles, if aws-role-arn is defined
        if (authScheme.equals(AWS_ROOT_KEYS) && awsRoleArn.isPresent()) {
            return AWS_IAM_ROLES;
        }
        return authScheme;
    }

    public Map<String, Object> getAwsRoleArnProperties()
    {
        return awsRoleArn.map(role ->
                        ImmutableMap.<String, Object>of(
                                AWS_ROLE_ARN, role,
                                CREDENTIALS_LOCATION, awsRoleCredentialsLocation))
                .orElse(ImmutableMap.of());
    }
}
