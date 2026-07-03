/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.substitution;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.Session;
import io.trino.SystemSessionPropertiesProvider;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;

import static io.trino.spi.session.PropertyMetadata.booleanProperty;

public final class MaterializedViewSubstitutionSessionProperties
        implements SystemSessionPropertiesProvider
{
    public static final String MATERIALIZED_VIEW_SUBSTITUTION_ENABLED = "materialized_view_substitution_enabled";

    private final List<PropertyMetadata<?>> sessionProperties;

    @VisibleForTesting
    public MaterializedViewSubstitutionSessionProperties()
    {
        this(new MaterializedViewSubstitutionConfig());
    }

    @Inject
    public MaterializedViewSubstitutionSessionProperties(MaterializedViewSubstitutionConfig config)
    {
        sessionProperties = ImmutableList.of(
                booleanProperty(
                        MATERIALIZED_VIEW_SUBSTITUTION_ENABLED,
                        "Enable automatic materialized view substitution in query plans",
                        config.isMaterializedViewSubstitutionEnabled(),
                        false));
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static boolean isMaterializedViewSubstitutionEnabled(Session session)
    {
        return session.getSystemProperty(MATERIALIZED_VIEW_SUBSTITUTION_ENABLED, Boolean.class);
    }
}
