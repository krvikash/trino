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
import com.google.inject.BindingAnnotation;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.starburstdata.trino.plugin.license.LicenseVerifier;
import dev.failsafe.RetryPolicy;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.RetryingConnectionFactory;
import io.trino.plugin.jdbc.credential.CredentialPropertiesProvider;
import io.trino.plugin.jdbc.jmx.StatisticsAwareConnectionFactory;
import io.trino.spi.connector.ConnectorPageSinkProvider;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.jdbc.JdbcModule.bindSessionPropertiesProvider;
import static io.trino.plugin.jdbc.JdbcModule.bindTablePropertiesProvider;
import static java.util.Objects.requireNonNull;

public class DynamoDbModule
        extends AbstractConfigurationAwareModule
{
    private final LicenseVerifier licenseVerifier;

    public DynamoDbModule(LicenseVerifier licenseVerifier)
    {
        this.licenseVerifier = requireNonNull(licenseVerifier, "licenseManager is null");
    }

    @Override
    protected void setup(Binder binder)
    {
        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class).to(DynamoDbJdbcClient.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(DynamoDbConfig.class);

        binder.bind(LicenseVerifier.class).toInstance(licenseVerifier);

        newOptionalBinder(binder, ConnectorPageSinkProvider.class).setBinding().to(DynamoDbJdbcPageSinkProvider.class).in(Scopes.SINGLETON);

        bindTablePropertiesProvider(binder, DynamoDbTableProperties.class);
        bindSessionPropertiesProvider(binder, DynamoDbSessionProperties.class);

        // Set the connection URL to some value as it is a required property in the JdbcModule
        // The actual connection URL is set via the DynamoDbConnectionFactory
        configBinder(binder).bindConfigDefaults(BaseJdbcConfig.class, config -> config.setConnectionUrl("jdbc:dynamodb:"));

        // Using optional binder for overriding ConnectionFactory in Galaxy
        newOptionalBinder(binder, Key.get(ConnectionFactory.class, ForBaseJdbc.class))
                .setDefault()
                .to(Key.get(ConnectionFactory.class, DefaultDynamoDbBinding.class))
                .in(Scopes.SINGLETON);

        install(new CredentialPropertiesModule());
    }

    @Provides
    @Singleton
    @DefaultDynamoDbBinding
    public ConnectionFactory getConnectionFactory(DynamoDbConfig dynamoDbConfig, CredentialPropertiesProvider credentialPropertiesProvider)
    {
        // The CData JDBC driver will intermittently throw an exception with no error message
        // Typically, retrying the query will cause it to proceed
        return new RetryingConnectionFactory(
                new StatisticsAwareConnectionFactory(
                        new DynamoDbConnectionFactory(dynamoDbConfig, credentialPropertiesProvider)),
                RetryPolicy.ofDefaults());
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target({ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD})
    @BindingAnnotation
    public @interface DefaultDynamoDbBinding {}
}
