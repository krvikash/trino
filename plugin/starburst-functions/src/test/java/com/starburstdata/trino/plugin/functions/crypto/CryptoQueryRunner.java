/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.functions.crypto;

import com.google.common.collect.ImmutableMap;
import com.starburstdata.trino.plugin.functions.FunctionsPlugin;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.Session;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;

import static com.starburstdata.trino.plugin.functions.crypto.CryptoFunctions.CRYPTO_SCHEMA_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;

/**
 * Catalog setup shared by the crypto tests, plus a {@code main} that starts a server for interactive
 * testing with the CLI or a client library. The TPCH catalog is attached so the functions can be
 * exercised against real rows rather than only literals.
 */
public final class CryptoQueryRunner
{
    public static final String STARBURST_CATALOG = "starburst";

    private CryptoQueryRunner() {}

    public static Session cryptoSession()
    {
        return testSessionBuilder()
                .setCatalog(STARBURST_CATALOG)
                .setSchema(CRYPTO_SCHEMA_NAME)
                .build();
    }

    public static void addStarburstCatalog(QueryRunner runner)
    {
        addStarburstCatalog(runner, true);
    }

    /**
     * Beyond {@code crypto.enabled} the crypto functions need no configuration, and
     * {@code ai.client.models.storage} defaults to {@code NONE}, so nothing else has to be set.
     */
    public static void addStarburstCatalog(QueryRunner runner, boolean cryptoEnabled)
    {
        runner.installPlugin(new FunctionsPlugin());
        runner.createCatalog(STARBURST_CATALOG, "starburst_functions", ImmutableMap.of("crypto.enabled", Boolean.toString(cryptoEnabled)));
    }

    public static QueryRunner createQueryRunner(int port)
            throws Exception
    {
        QueryRunner queryRunner = DistributedQueryRunner.builder(cryptoSession())
                .addCoordinatorProperty("http-server.http.port", Integer.toString(port))
                .build();
        try {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch", ImmutableMap.of());
            addStarburstCatalog(queryRunner);
            return queryRunner;
        }
        catch (Exception e) {
            queryRunner.close();
            throw e;
        }
    }

    static void main()
            throws Exception
    {
        Logging.initialize();
        QueryRunner queryRunner = createQueryRunner(8080);
        Logger log = Logger.get(CryptoQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
