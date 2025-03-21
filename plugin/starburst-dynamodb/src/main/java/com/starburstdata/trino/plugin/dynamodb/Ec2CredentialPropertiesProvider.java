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

import static com.starburstdata.trino.plugin.dynamodb.DynamoDbConnectorPropertiesConstants.AUTH_SCHEME;
import static com.starburstdata.trino.plugin.dynamodb.DynamoDbConnectorPropertiesConstants.AWS_EC2_ROLES;
import static java.util.Objects.requireNonNull;

public class Ec2CredentialPropertiesProvider
        implements CredentialPropertiesProvider
{
    private final AwsRolePropertiesProvider awsRolePropertiesProvider;

    @Inject
    public Ec2CredentialPropertiesProvider(AwsRolePropertiesProvider awsRolePropertiesProvider)
    {
        this.awsRolePropertiesProvider = requireNonNull(awsRolePropertiesProvider, "awsRolePropertiesProvider is null");
    }

    @Override
    public Map<String, Object> getCredentialProperties(ConnectorIdentity identity)
    {
        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
        properties.put(AUTH_SCHEME, awsRolePropertiesProvider.getAuthScheme(AWS_EC2_ROLES));
        properties.putAll(awsRolePropertiesProvider.getAwsRoleArnProperties());
        return properties.buildOrThrow();
    }
}
