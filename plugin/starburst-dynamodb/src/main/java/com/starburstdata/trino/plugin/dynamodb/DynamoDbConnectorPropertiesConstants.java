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

public final class DynamoDbConnectorPropertiesConstants
{
    private DynamoDbConnectorPropertiesConstants() {}

    public static final String AUTH_SCHEME = "AuthScheme";
    public static final String AWS_ACCESS_KEY = "AWS Access Key";
    public static final String AWS_SECRET_KEY = "AWS Secret Key";
    public static final String AWS_SESSION_TOKEN = "AWSSessionToken";
    public static final String AWS_ROLE_ARN = "AWS Role Arn";
    public static final String CREDENTIALS_LOCATION = "CredentialsLocation";

    public static final String TEMPORARY_CREDENTIALS = "TemporaryCredentials";
    public static final String AWS_EC2_ROLES = "AwsEC2Roles";
    public static final String AWS_IAM_ROLES = "AwsIAMRoles";
    public static final String AWS_ROOT_KEYS = "AwsRootKeys";
}
