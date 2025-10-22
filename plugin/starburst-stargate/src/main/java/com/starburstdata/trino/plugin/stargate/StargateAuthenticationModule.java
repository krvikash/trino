/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.stargate;

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.jdbc.TrinoDriver;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.jdbc.credential.CredentialProviderModule;

import java.util.Properties;

import static com.starburstdata.trino.plugin.stargate.StargateConfig.PASSWORD;
import static com.starburstdata.trino.plugin.stargate.TrinoUriFactory.sslConnectionProperties;
import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.client.uri.PropertyName.ENCODING;

public class StargateAuthenticationModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        install(conditionalModule(
                StargateConfig.class,
                config -> PASSWORD.equalsIgnoreCase(config.getAuthenticationType()),
                new PasswordModule()));
    }

    private static class PasswordModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        protected void setup(Binder binder)
        {
            install(new CredentialProviderModule());
            configBinder(binder).bindConfig(StargateCredentialConfig.class);
            binder.bind(StargateCatalogIdentityFactory.class)
                    .to(PasswordCatalogIdentityFactory.class)
                    .in(Scopes.SINGLETON);
        }

        @Provides
        @Singleton
        @TransportConnectionFactory
        public ConnectionFactory getConnectionFactory(
                BaseJdbcConfig config,
                StargateConfig connectorConfig,
                StargateSslConfig sslConfig,
                CredentialProvider credentialProvider,
                @AllowForSpoolingProtocol boolean withSpooling)
        {
            Properties connectionProperties = new Properties();
            connectionProperties.putAll(sslConnectionProperties(connectorConfig, sslConfig));

            if (!withSpooling) {
                connectionProperties.put(ENCODING.toString(), ""); // Always negotiate direct protocol
            }

            return DriverConnectionFactory.builder(new TrinoDriver(), config.getConnectionUrl(), credentialProvider)
                    .setConnectionProperties(connectionProperties)
                    .build();
        }
    }
}
