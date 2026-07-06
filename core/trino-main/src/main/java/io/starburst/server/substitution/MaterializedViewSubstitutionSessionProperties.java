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
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.SystemSessionPropertiesProvider;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

import static io.trino.plugin.base.session.PropertyMetadataUtil.durationProperty;
import static io.trino.spi.session.PropertyMetadata.booleanProperty;
import static io.trino.spi.session.PropertyMetadata.stringProperty;

public final class MaterializedViewSubstitutionSessionProperties
        implements SystemSessionPropertiesProvider
{
    public static final String MATERIALIZED_VIEW_SUBSTITUTION_ENABLED = "materialized_view_substitution_enabled";
    public static final String MATERIALIZED_VIEW_SUBSTITUTION_MAX_STALENESS = "materialized_view_substitution_max_staleness";
    public static final String MATERIALIZED_VIEW_SUBSTITUTION_CANDIDATES_REGEX_FILTER = "materialized_view_substitution_candidates_regex_filter";

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
                        false),
                durationProperty(
                        MATERIALIZED_VIEW_SUBSTITUTION_MAX_STALENESS,
                        "Maximum staleness of a materialized view eligible for substitution; unset means only each materialized view's own grace period applies",
                        config.getMaterializedViewSubstitutionMaxStaleness().orElse(null),
                        false),
                stringProperty(
                        MATERIALIZED_VIEW_SUBSTITUTION_CANDIDATES_REGEX_FILTER,
                        "Regular expression matched against the fully qualified materialized view name (catalog.schema.table); unset means all materialized views are eligible for substitution",
                        config.getMaterializedViewSubstitutionCandidatesRegexFilter().orElse(null),
                        MaterializedViewSubstitutionSessionProperties::validatePattern,
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

    public static Optional<Duration> getMaterializedViewSubstitutionMaxStaleness(Session session)
    {
        return Optional.ofNullable(session.getSystemProperty(MATERIALIZED_VIEW_SUBSTITUTION_MAX_STALENESS, Duration.class));
    }

    public static Optional<String> getMaterializedViewSubstitutionCandidatesRegexFilter(Session session)
    {
        return Optional.ofNullable(session.getSystemProperty(MATERIALIZED_VIEW_SUBSTITUTION_CANDIDATES_REGEX_FILTER, String.class));
    }

    private static void validatePattern(String pattern)
    {
        Pattern.compile(pattern);
    }
}
