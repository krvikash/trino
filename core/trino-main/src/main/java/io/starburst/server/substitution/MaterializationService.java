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

import io.trino.Session;
import io.trino.metadata.QualifiedObjectName;

import java.util.Map;
import java.util.Optional;

public interface MaterializationService
{
    /**
     * Verifies that a materialized view with the given definition query can be used as a substitution
     * target, so that enabling {@code substitution_enabled} on a view that would never be indexed fails
     * the DDL instead of silently leaving the property inert.
     *
     * @throws io.trino.spi.TrinoException if the definition query cannot drive substitution
     */
    void validateSubstitutionCandidate(Session session, QualifiedObjectName viewName, String originalSql);

    void setMaterializedViewProperties(Session session, QualifiedObjectName viewName, Map<String, Optional<Object>> properties);

    void finishRefreshMaterializedView(Session session, QualifiedObjectName materializedViewName);

    void renameIfExists(Session session, QualifiedObjectName source, QualifiedObjectName target);

    void remove(QualifiedObjectName materializedViewName);
}
