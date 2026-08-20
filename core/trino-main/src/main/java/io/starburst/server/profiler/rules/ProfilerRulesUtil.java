/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.profiler.rules;

import io.trino.sql.planner.plan.TableScanNode;

public final class ProfilerRulesUtil
{
    private ProfilerRulesUtil() {}

    public static boolean isJdbc(TableScanNode tableScanNode)
    {
        return tableScanNode.getTable().connectorHandle().getClass().getName().endsWith("JdbcTableHandle");
    }
}
