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

import com.google.inject.Inject;
import io.trino.dispatcher.DispatchManager;
import io.trino.execution.QueryInfo;
import io.trino.spi.QueryId;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class FullQueryInfoProviderDispatchManager
        implements FullQueryInfoProvider
{
    private final DispatchManager dispatchManager;

    @Inject
    public FullQueryInfoProviderDispatchManager(DispatchManager dispatchManager)
    {
        this.dispatchManager = requireNonNull(dispatchManager, "dispatchManager is null");
    }

    @Override
    public Optional<QueryInfo> getFullQueryInfo(QueryId queryId)
    {
        return dispatchManager.getFullQueryInfo(queryId);
    }
}
