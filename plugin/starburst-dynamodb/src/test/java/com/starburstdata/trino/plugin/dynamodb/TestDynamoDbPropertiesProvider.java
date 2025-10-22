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
import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.trino.plugin.base.ConnectorContextModule;
import io.trino.plugin.jdbc.JdbcModule;
import io.trino.plugin.jdbc.credential.CredentialPropertiesProvider;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.TestingConnectorContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

import java.nio.file.Path;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

final class TestDynamoDbPropertiesProvider
{
    @Test
    void testEmptyConfig(@TempDir Path tempDir)
    {
        Map<String, String> inputConfig = ImmutableMap.of(
                "dynamodb.aws-region", "us-east-2",
                "dynamodb.schema-directory", tempDir.toFile().getAbsolutePath());
        Map<String, String> expectedOutputProperties = ImmutableMap.of("AuthScheme", "AwsEC2Roles");

        testCredentialsPropertiesProvider(inputConfig, DefaultCredentialsProvider.create(), expectedOutputProperties);
    }

    @Test
    void testOnlyRole(@TempDir Path tempDir)
    {
        Map<String, String> inputConfig = ImmutableMap.of(
                "dynamodb.aws-region", "us-east-2",
                "dynamodb.schema-directory", tempDir.toFile().getAbsolutePath(),
                "dynamodb.aws-role-arn", "roleArn",
                "dynamodb.aws-role-credentials-location", "credential_location");

        Map<String, String> expectedOutputProperties = ImmutableMap.of(
                "AWS Role Arn", "roleArn",
                "AuthScheme", "AwsEC2Roles",
                "CredentialsLocation", "credential_location");

        testCredentialsPropertiesProvider(inputConfig, DefaultCredentialsProvider.create(), expectedOutputProperties);
    }

    @Test
    void testPropertiesKeys(@TempDir Path tempDir)
    {
        Map<String, String> inputConfig = ImmutableMap.of(
                "dynamodb.aws-region", "us-east-2",
                "dynamodb.schema-directory", tempDir.toFile().getAbsolutePath(),
                "dynamodb.aws-secret-key", "secretKey",
                "dynamodb.aws-access-key", "accessKey");

        Map<String, String> expectedOutputProperties = ImmutableMap.of(
                "AWS Access Key", "accessKey",
                "AWS Secret Key", "secretKey",
                "AuthScheme", "AwsRootKeys");

        testCredentialsPropertiesProvider(inputConfig, DefaultCredentialsProvider.create(), expectedOutputProperties);
    }

    @Test
    void testPropertiesKeysAndRole(@TempDir Path tempDir)
    {
        Map<String, String> inputConfig = ImmutableMap.of(
                "dynamodb.aws-region", "us-east-2",
                "dynamodb.schema-directory", tempDir.toFile().getAbsolutePath(),
                "dynamodb.aws-secret-key", "secretKey",
                "dynamodb.aws-access-key", "accessKey",
                "dynamodb.aws-role-arn", "roleArn",
                "dynamodb.aws-role-credentials-location", "credential_location");

        Map<String, String> expectedOutputProperties = ImmutableMap.of(
                "AWS Access Key", "accessKey",
                "AWS Role Arn", "roleArn",
                "AWS Secret Key", "secretKey",
                "AuthScheme", "AwsIAMRoles",
                "CredentialsLocation", "credential_location");

        testCredentialsPropertiesProvider(inputConfig, DefaultCredentialsProvider.create(), expectedOutputProperties);
    }

    @Test
    void testDefaultChain(@TempDir Path tempDir)
    {
        AwsBasicCredentials credentials = AwsBasicCredentials.create("keyFromChain", "secretFromChain");
        Map<String, String> inputConfig = ImmutableMap.of(
                "dynamodb.aws-region", "us-east-2",
                "dynamodb.schema-directory", tempDir.toFile().getAbsolutePath(),
                "dynamodb.use-default-aws-chain-provider", "true");

        Map<String, String> expectedOutputProperties = ImmutableMap.of(
                "AWS Access Key", "keyFromChain",
                "AWS Secret Key", "secretFromChain",
                "AuthScheme", "AwsRootKeys");

        testCredentialsPropertiesProvider(inputConfig, StaticCredentialsProvider.create(credentials), expectedOutputProperties);
    }

    @Test
    void testDefaultChainWithRole(@TempDir Path tempDir)
    {
        AwsBasicCredentials credentials = AwsBasicCredentials.create("keyFromChain", "secretFromChain");
        Map<String, String> inputConfig = ImmutableMap.of(
                "dynamodb.aws-region", "us-east-2",
                "dynamodb.schema-directory", tempDir.toFile().getAbsolutePath(),
                "dynamodb.use-default-aws-chain-provider", "true",
                "dynamodb.aws-role-arn", "roleArn",
                "dynamodb.aws-role-credentials-location", "credential_location");

        Map<String, String> expectedOutputProperties = ImmutableMap.of(
                "AWS Access Key", "keyFromChain",
                "AWS Role Arn", "roleArn",
                "AWS Secret Key", "secretFromChain",
                "AuthScheme", "AwsIAMRoles",
                "CredentialsLocation", "credential_location");

        testCredentialsPropertiesProvider(inputConfig, StaticCredentialsProvider.create(credentials), expectedOutputProperties);
    }

    @Test
    void testDefaultSessionChain(@TempDir Path tempDir)
    {
        AwsSessionCredentials credentials = AwsSessionCredentials.create("keyFromChain", "secretFromChain", "sessionToken");
        Map<String, String> inputConfig = ImmutableMap.of(
                "dynamodb.aws-region", "us-east-2",
                "dynamodb.schema-directory", tempDir.toFile().getAbsolutePath(),
                "dynamodb.use-default-aws-chain-provider", "true");

        Map<String, String> expectedOutputProperties = ImmutableMap.of(
                "AWS Access Key", "keyFromChain",
                "AWS Secret Key", "secretFromChain",
                "AWSSessionToken", "sessionToken",
                "AuthScheme", "TemporaryCredentials");

        testCredentialsPropertiesProvider(inputConfig, StaticCredentialsProvider.create(credentials), expectedOutputProperties);
    }

    @Test
    void testDefaultSessionChainWithRole(@TempDir Path tempDir)
    {
        AwsSessionCredentials credentials = AwsSessionCredentials.create("keyFromChain", "secretFromChain", "sessionToken");
        Map<String, String> inputConfig = ImmutableMap.of(
                "dynamodb.aws-region", "us-east-2",
                "dynamodb.schema-directory", tempDir.toFile().getAbsolutePath(),
                "dynamodb.use-default-aws-chain-provider", "true",
                "dynamodb.aws-role-arn", "roleArn",
                "dynamodb.aws-role-credentials-location", "credential_location");

        Map<String, String> expectedOutputProperties = ImmutableMap.of(
                "AWS Access Key", "keyFromChain",
                "AWS Role Arn", "roleArn",
                "AWS Secret Key", "secretFromChain",
                "AWSSessionToken", "sessionToken",
                "AuthScheme", "TemporaryCredentials",
                "CredentialsLocation", "credential_location");

        testCredentialsPropertiesProvider(inputConfig, StaticCredentialsProvider.create(credentials), expectedOutputProperties);
    }

    private void testCredentialsPropertiesProvider(Map<String, String> inputConfig, AwsCredentialsProvider credentialsProvider, Map<String, String> outputProperties)
    {
        ConnectorContext context = new TestingConnectorContext();
        Injector injector = new Bootstrap(
                new JdbcModule(),
                new DynamoDbModule(() -> true),
                new ConnectorContextModule("test", context),
                binder -> binder.bind(Boolean.class).annotatedWith(EnableWrites.class).toInstance(false),
                binder -> binder.bind(AwsCredentialsProvider.class).toInstance(credentialsProvider))
                .setRequiredConfigurationProperties(inputConfig)
                .initialize();

        CredentialPropertiesProvider credentialPropertiesProvider = injector.getInstance(CredentialPropertiesProvider.class);
        Map<String, Object> properties = credentialPropertiesProvider.getCredentialProperties(ConnectorIdentity.ofUser("test"));

        assertThat(properties).isEqualTo(outputProperties);
    }
}
