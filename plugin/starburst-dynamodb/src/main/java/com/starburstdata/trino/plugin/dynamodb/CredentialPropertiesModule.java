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
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.jdbc.credential.CredentialPropertiesProvider;

import static com.google.inject.Scopes.SINGLETON;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConditionalModule.conditionalModule;

public class CredentialPropertiesModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        newOptionalBinder(binder, CredentialPropertiesProvider.class).setDefault().to(Ec2CredentialPropertiesProvider.class).in(Scopes.SINGLETON);

        install(conditionalModule(
                DynamoDbConfig.class,
                config -> config.getAwsAccessKey().isPresent(),
                internalBinder ->
                        newOptionalBinder(internalBinder, CredentialPropertiesProvider.class)
                                .setBinding()
                                .to(ConfigCredentialPropertiesProvider.class)
                                .in(SINGLETON)));

        install(conditionalModule(
                DynamoDbConfig.class,
                DynamoDbConfig::isUseDefaultAwsChainProvider,
                internalBinder ->
                        newOptionalBinder(internalBinder, CredentialPropertiesProvider.class)
                                .setBinding()
                                .to(AwsChainCredentialPropertiesProvider.class)
                                .in(SINGLETON)));

        binder.bind(AwsRolePropertiesProvider.class).in(SINGLETON);
    }
}
