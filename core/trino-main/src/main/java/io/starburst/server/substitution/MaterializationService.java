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
    void setMaterializedViewProperties(Session session, QualifiedObjectName viewName, Map<String, Optional<Object>> properties);

    void finishRefreshMaterializedView(Session session, QualifiedObjectName materializedViewName);

    void renameIfExists(Session session, QualifiedObjectName source, QualifiedObjectName target);

    void remove(QualifiedObjectName materializedViewName);
}
