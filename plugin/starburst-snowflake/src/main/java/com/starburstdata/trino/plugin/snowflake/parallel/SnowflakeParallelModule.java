/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.snowflake.parallel;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.starburstdata.trino.plugin.snowflake.SnowflakeConfig;
import com.starburstdata.trino.plugin.snowflake.SnowflakeProxyConfig;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.jdbc.JdbcSplitManager;
import io.trino.spi.connector.ConnectorSplitManager;
import net.snowflake.client.internal.core.HttpClientSettingsKey;
import net.snowflake.client.internal.core.HttpUtil;
import net.snowflake.client.internal.core.OCSPMode;
import org.apache.http.impl.client.CloseableHttpClient;

import static com.google.inject.Scopes.SINGLETON;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.jdbc.JdbcModule.bindSessionPropertiesProvider;
import static java.lang.String.join;
import static java.util.Locale.ENGLISH;

public class SnowflakeParallelModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        binder.bind(SnowflakeParallelSplitSourceFactory.class).in(SINGLETON);
        newOptionalBinder(binder, ConnectorSplitManager.class)
                .setBinding().to(SnowflakeSplitManager.class).in(SINGLETON);
        bindSessionPropertiesProvider(binder, SnowflakeParallelSessionProperties.class);
        binder.bind(SnowflakeParallelConnector.class).in(SINGLETON);
        binder.bind(JdbcSplitManager.class).in(SINGLETON);

        binder.bind(StarburstResultStreamProvider.class).in(SINGLETON);
        if (buildConfigObject(SnowflakeConfig.class).isProxyEnabled()) {
            binder.install(new ProxiedHttpModule());
        }
        else {
            binder.install(new DefaultHttpModule());
        }
    }

    public static class DefaultHttpModule
            implements Module
    {
        @Override
        public void configure(Binder binder) {}

        @Provides
        @Singleton
        public static CloseableHttpClient getHttpClient()
        {
            return HttpUtil.getHttpClient(new HttpClientSettingsKey(OCSPMode.FAIL_OPEN));
        }
    }

    public static class ProxiedHttpModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            configBinder(binder).bindConfig(SnowflakeProxyConfig.class);
        }

        @Provides
        @Singleton
        public static CloseableHttpClient getHttpClient(SnowflakeProxyConfig snowflakeProxyConfig)
        {
            return HttpUtil.getHttpClient(new HttpClientSettingsKey(
                    OCSPMode.FAIL_OPEN,
                    snowflakeProxyConfig.getProxyHost(),
                    snowflakeProxyConfig.getProxyPort(),
                    // see https://docs.snowflake.com/en/developer-guide/jdbc/jdbc-configure#bypassing-the-proxy-server
                    join("%7C", snowflakeProxyConfig.getNonProxyHosts()),
                    snowflakeProxyConfig.getUsername().orElse(null),
                    snowflakeProxyConfig.getPassword().orElse(null),
                    snowflakeProxyConfig.getProxyProtocol().name().toLowerCase(ENGLISH),
                    null,
                    false));
        }
    }
}
