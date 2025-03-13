/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.snowflake;

import io.airlift.log.Logger;
import io.netty.handler.codec.http.HttpRequest;
import io.trino.testing.QueryRunner;
import org.littleshoot.proxy.ActivityTrackerAdapter;
import org.littleshoot.proxy.FlowContext;
import org.littleshoot.proxy.HttpProxyServer;
import org.littleshoot.proxy.ProxyAuthenticator;
import org.littleshoot.proxy.impl.DefaultHttpProxyServer;

import java.io.Closeable;
import java.util.Map;
import java.util.Optional;

import static com.starburstdata.trino.plugin.snowflake.SnowflakeQueryRunner.TEST_SCHEMA;
import static com.starburstdata.trino.plugin.snowflake.SnowflakeQueryRunner.impersonationDisabled;
import static com.starburstdata.trino.plugin.snowflake.SnowflakeQueryRunner.parallelBuilder;

public class TestParallelSnowflakeWithProxyConnectorSmokeTest
        extends BaseSnowflakeConnectorSmokeTest
{
    private static final Logger log = Logger.get(TestParallelSnowflakeWithProxyConnectorSmokeTest.class);
    private static final String PROXY_USER = "proxyuser";
    private static final String PROXY_PASSWORD = "proxypassword";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        closeAfterClass(createProxyServer());
        return parallelBuilder()
                .withDatabase(Optional.of(getTestDatabase().getName()))
                .withSchema(Optional.of(TEST_SCHEMA))
                .withConnectorProperties(impersonationDisabled())
                .withConnectorProperties(Map.of(
                        "snowflake.proxy.enabled", "true",
                        "snowflake.proxy.host", "localhost",
                        "snowflake.proxy.port", "8888",
                        "snowflake.proxy.protocol", "http",
                        "snowflake.proxy.username", PROXY_USER,
                        "snowflake.proxy.password", PROXY_PASSWORD))
                .withTpchTables(REQUIRED_TPCH_TABLES)
                .build();
    }

    private Closeable createProxyServer()
    {
        return new CloseableProxyServer(DefaultHttpProxyServer.bootstrap()
                .withPort(8888)
                .withTransparent(true)
                .plusActivityTracker(new ActivityTrackerAdapter()
                {
                    @Override
                    public void requestReceivedFromClient(FlowContext flowContext, HttpRequest httpRequest)
                    {
                        log.info("Proxying request to " + httpRequest.uri());
                    }
                })
                .withProxyAuthenticator(new ProxyAuthenticator()
                {
                    @Override
                    public boolean authenticate(String userName, String password)
                    {
                        return userName.contentEquals(PROXY_USER) && password.contentEquals(PROXY_PASSWORD);
                    }

                    @Override
                    public String getRealm()
                    {
                        return null;
                    }
                })
                .start());
    }

    @SuppressWarnings("UnusedVariable")
    private record CloseableProxyServer(HttpProxyServer proxy)
            implements Closeable
    {
        @Override
        public void close()
        {
            proxy.stop();
        }
    }
}
