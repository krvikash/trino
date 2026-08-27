/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.salesforce;

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.salesforce.cdp.queryservice.QueryServiceDriver;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.plugin.base.mapping.CachingIdentifierMapping;
import io.trino.plugin.base.mapping.IdentifierMapping;
import io.trino.plugin.base.mapping.MappingConfig;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DecimalModule;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.ExtraCredentialsBasedIdentityCacheMappingModule;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.jdbc.credential.CredentialProviderModule;

import java.util.Properties;

import static com.google.inject.Scopes.SINGLETON;
import static com.salesforce.cdp.queryservice.util.Constants.CLIENT_ID;
import static com.salesforce.cdp.queryservice.util.Constants.CLIENT_SECRET;
import static com.salesforce.cdp.queryservice.util.Constants.PD;
import static com.salesforce.cdp.queryservice.util.Constants.USER;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class SalesforceCdpModule
        extends AbstractConfigurationAwareModule
{
    @SuppressWarnings("deprecation") // TODO https://starburstdata.atlassian.net/browse/ENG-9403 Revise Salesforce decimal mapping with Trino NUMBER
    @Override
    protected void setup(Binder binder)
    {
        MappingConfig mappingConfig = buildConfigObject(MappingConfig.class);
        // CachingIdentifierMapping is used to case-insensitive name matching and SalesforceCdpIdentifierMapping is used to strip Salesforce specific suffixes (__dll, __c).
        // This mapping is similar to isCaseInsensitiveNameMatching in IdentifierMappingModule,
        // but instead of DefaultIdentifierMapping we provide SalesforceCdpIdentifierMapping to CachingIdentifierMapping
        binder.bind(IdentifierMapping.class)
                .annotatedWith(ForSalesforceCdpIdentifierMapping.class)
                .toProvider(() -> new CachingIdentifierMapping(mappingConfig, new SalesforceCdpIdentifierMapping()))
                .in(SINGLETON);

        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class).to(SalesforceCdpJdbcClient.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(SalesforceCdpConfig.class);

        install(new CredentialProviderModule());
        install(new ExtraCredentialsBasedIdentityCacheMappingModule());

        install(new DecimalModule());
        // Correct URL cannot be set as property in the JdbcModule as it fails validation because of '-', set dummy value here.
        // The actual connection URL is set in getConnectionFactory.
        configBinder(binder).bindConfigDefaults(BaseJdbcConfig.class, config -> config.setConnectionUrl("jdbc:queryService:jdbc"));
    }

    @Provides
    @Singleton
    @ForBaseJdbc
    public ConnectionFactory getConnectionFactory(CredentialProvider credentialProvider, SalesforceCdpConfig salesforceCdpConfig, OpenTelemetry openTelemetry)
    {
        Properties properties = new Properties();
        // the driver needs all these as properties. Does not work if we provide them in the url
        properties.put(USER, salesforceCdpConfig.getUser());
        properties.put(PD, salesforceCdpConfig.getPassword());
        properties.put(CLIENT_ID, salesforceCdpConfig.getClientId());
        properties.put(CLIENT_SECRET, salesforceCdpConfig.getClientSecret());
        // TODO: Override socket connection for region enforcement once https://github.com/forcedotcom/Salesforce-CDP-jdbc/issues/166 is fixed
        return DriverConnectionFactory.builder(new QueryServiceDriver(), "jdbc:queryService-jdbc:https://login.salesforce.com/", credentialProvider)
                .setOpenTelemetry(openTelemetry)
                .setConnectionProperties(properties)
                .build();
    }
}
