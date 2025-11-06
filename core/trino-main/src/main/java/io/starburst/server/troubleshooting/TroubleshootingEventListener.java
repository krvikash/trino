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
import io.starburst.server.troubleshooting.tracing.SpanInterceptor;
import io.trino.eventlistener.EventListenerManager;
import io.trino.spi.QueryId;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryContext;
import io.trino.spi.eventlistener.QueryCreatedEvent;

import static io.trino.client.AdditionalClientCapabilities.QUERY_TROUBLESHOOTING;
import static io.trino.spi.security.Identity.forUser;
import static java.util.Objects.requireNonNull;

public class TroubleshootingEventListener
        implements EventListener
{
    private final TroubleshootingAccessControl accessControl;
    private final TroubleshootingContextManager troubleshootingContextManager;
    private final boolean anonymizePlan;
    private final SpanInterceptor spanInterceptor;

    @Inject
    public TroubleshootingEventListener(
            TroubleshootingAccessControl accessControl,
            TroubleshootingConfig config,
            TroubleshootingContextManager troubleshootingContextManager,
            EventListenerManager listenerManager,
            SpanInterceptor spanInterceptor)
    {
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.anonymizePlan = requireNonNull(config, "config is null").isAnonymizedPlan();
        this.troubleshootingContextManager = requireNonNull(troubleshootingContextManager, "troubleshootingContextManager is null");
        requireNonNull(listenerManager, "listenerManager is null").addEventListener(this);
        this.spanInterceptor = requireNonNull(spanInterceptor, "spanInterceptor is null");
    }

    @Override
    public void queryCreated(QueryCreatedEvent queryCreatedEvent)
    {
        if (!isTroubleshootingEnabled(queryCreatedEvent.getContext())) {
            return;
        }

        // queryCreated is fired before query has even started planning. We need to eagerly start collecting data,
        // to stop and discard immediately when we learn which nodes are actually processing this query.
        troubleshootingContextManager.start(QueryId.valueOf(queryCreatedEvent.getMetadata().getQueryId()));
    }

    @Override
    public void queryCompleted(QueryCompletedEvent event)
    {
        spanInterceptor.forgetTracking(QueryId.valueOf(event.getMetadata().getQueryId()));

        if (!isTroubleshootingEnabled(event.getContext())) {
            return;
        }
        troubleshootingContextManager.finish(QueryId.valueOf(event.getMetadata().getQueryId()));
    }

    public boolean isTroubleshootingEnabled(QueryContext context)
    {
        if (!context.getClientCapabilities().contains(QUERY_TROUBLESHOOTING.name())) {
            return false;
        }
        return accessControl.canRunAndTroubleshoot(forUser(context.getUser())
                .withEnabledRoles(context.getEnabledRoles())
                .withGroups(context.getGroups())
                .build());
    }

    @Override
    public boolean requiresAnonymizedPlan()
    {
        return anonymizePlan;
    }
}
