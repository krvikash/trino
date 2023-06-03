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
import com.starburstdata.presto.server.security.webui.access.WebUiAccessControl;
import io.trino.eventlistener.EventListenerManager;
import io.trino.spi.QueryId;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryContext;
import io.trino.spi.eventlistener.QueryCreatedEvent;

import static com.starburstdata.presto.server.StarburstClientCapabilities.QUERY_TROUBLESHOOTING;
import static io.trino.spi.security.Identity.forUser;
import static java.util.Objects.requireNonNull;

public class TroubleshootingEventListener
        implements EventListener
{
    private final WebUiAccessControl accessControl;
    private final TroubleshootingManager troubleshootingManager;
    private final boolean anonymizePlan;

    @Inject
    public TroubleshootingEventListener(WebUiAccessControl accessControl, TroubleshootingConfig config, TroubleshootingManager troubleshootingManager, EventListenerManager listenerManager)
    {
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.anonymizePlan = requireNonNull(config, "config is null").isAnonymizedPlan();
        this.troubleshootingManager = requireNonNull(troubleshootingManager, "troubleshootingManager is null");
        requireNonNull(listenerManager, "listenerManager is null").addEventListener(this);
    }

    @Override
    public void queryCreated(QueryCreatedEvent queryCreatedEvent)
    {
        if (!isTroubleshootingEnabled(queryCreatedEvent.getContext())) {
            return;
        }

        // queryCreated is fired before query has even started planning. We need to eagerly start collecting data,
        // to stop and discard immediately when we learn which nodes are actually processing this query.
        troubleshootingManager.start(QueryId.valueOf(queryCreatedEvent.getMetadata().getQueryId()));
    }

    @Override
    public void queryCompleted(QueryCompletedEvent event)
    {
        if (!isTroubleshootingEnabled(event.getContext())) {
            return;
        }
        troubleshootingManager.finish(QueryId.valueOf(event.getMetadata().getQueryId()));
    }

    public boolean isTroubleshootingEnabled(QueryContext context)
    {
        if (!context.getClientCapabilities().contains(QUERY_TROUBLESHOOTING.name())) {
            return false;
        }
        return accessControl.isPrivilegedUser(forUser(context.getUser()).build());
    }

    @Override
    public boolean requiresAnonymizedPlan()
    {
        return anonymizePlan;
    }
}
