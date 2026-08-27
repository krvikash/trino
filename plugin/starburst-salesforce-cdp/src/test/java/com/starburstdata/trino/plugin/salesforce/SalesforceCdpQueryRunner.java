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

import io.airlift.log.Level;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.testing.DistributedQueryRunner;

import java.util.HashMap;
import java.util.Map;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;

public final class SalesforceCdpQueryRunner
{
    private SalesforceCdpQueryRunner() {}

    public static Builder builder()
    {
        return new Builder()
                .addConnectorProperties(Map.of(
                        "salesforce-cdp.user", requireNonNull(System.getProperty("salesforce-cdp.test.basic.auth.user"), "salesforce-cdp.test.basic.auth.user is not set"),
                        "salesforce-cdp.password", requireNonNull(System.getProperty("salesforce-cdp.test.basic.auth.password"), "salesforce-cdp.test.basic.auth.password is not set"),
                        "salesforce-cdp.oauth2.client-id", requireNonNull(System.getProperty("salesforce-cdp.test.oauth2.client-id"), "salesforce-cdp.test.oauth2.client-id is not set"),
                        "salesforce-cdp.oauth2.client-secret", requireNonNull(System.getProperty("salesforce-cdp.test.oauth2.client-secret"), "salesforce-cdp.test.oauth2.client-secret is not set"),
                        "case-insensitive-name-matching", "true"));
    }

    public static final class Builder
            extends DistributedQueryRunner.Builder<Builder>
    {
        private static final String CATALOG_NAME = "salesforce_cdp";
        private final Map<String, String> connectorProperties = new HashMap<>();

        private Builder()
        {
            super(testSessionBuilder()
                    .setCatalog(CATALOG_NAME)
                    .setSchema("queryservice")
                    .build());
        }

        public Builder addConnectorProperties(Map<String, String> properties)
        {
            connectorProperties.putAll(properties);
            return this;
        }

        @Override
        public DistributedQueryRunner build()
                throws Exception
        {
            DistributedQueryRunner queryRunner = super.build();
            try {
                queryRunner.installPlugin(new SalesforceCdpPlugin());
                queryRunner.createCatalog(CATALOG_NAME, "salesforce_cdp", connectorProperties);

                return queryRunner;
            }
            catch (Throwable e) {
                closeAllSuppress(e, queryRunner);
                throw e;
            }
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        Logging.initialize().setRootLevel(Level.INFO);

        DistributedQueryRunner queryRunner = SalesforceCdpQueryRunner.builder()
                .addCoordinatorProperty("http-server.http.port", "8080")
                .build();

        Logger log = Logger.get(SalesforceCdpQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
