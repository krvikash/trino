/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.troubleshooting;

import io.trino.execution.QueryInfo;
import io.trino.spi.QueryId;

import java.util.Optional;

public interface FullQueryInfoProvider
{
    Optional<QueryInfo> getFullQueryInfo(QueryId queryId);
}
