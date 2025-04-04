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

import com.google.inject.Binder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class AwsCredentialModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(DynamoDbConfig.class);

        newOptionalBinder(binder, AwsCredentialsProvider.class);

        install(conditionalModule(
                DynamoDbConfig.class,
                DynamoDbConfig::isUseDefaultAwsChainProvider,
                awsChainCredentialsBinder ->
                        newOptionalBinder(awsChainCredentialsBinder, AwsCredentialsProvider.class)
                                .setBinding()
                                .toInstance(DefaultCredentialsProvider.create())));
    }
}
