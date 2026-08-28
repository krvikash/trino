/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.castleinsights;

import com.google.common.collect.ImmutableMap;
import io.trino.execution.QueryInfo;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.QueryId;
import io.trino.testing.StandaloneQueryRunner;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static io.trino.SessionTestUtils.TEST_SESSION;

public final class CastleInsightsTestHelper
{
    private CastleInsightsTestHelper() {}

    /**
     * A standalone runner with the TPC-H catalog, the fixture the analyzer tests plan against.
     */
    public static StandaloneQueryRunner createTpchQueryRunner()
    {
        StandaloneQueryRunner queryRunner = new StandaloneQueryRunner(TEST_SESSION);
        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog(TEST_SESSION.getCatalog().orElseThrow(), "tpch", ImmutableMap.of());
        return queryRunner;
    }

    /**
     * The final {@link QueryInfo} for a completed query, waiting briefly for it to be published.
     */
    public static QueryInfo finalQueryInfo(StandaloneQueryRunner queryRunner, QueryId queryId)
    {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<QueryInfo> queryInfo = new AtomicReference<>();
        queryRunner.getCoordinator().addFinalQueryInfoListener(queryId, info -> {
            queryInfo.set(info);
            latch.countDown();
        });
        try {
            if (!latch.await(30, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Timed out waiting for final query info for query " + queryId);
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        return queryInfo.get();
    }
}
