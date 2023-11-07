/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.troubleshooting.jmx;

import java.util.HashMap;
import java.util.Map;

final class JmxTroubleshootingContext
{
    private final Map<String, Map<String, Object>> before = new HashMap<>();
    private final Map<String, Map<String, Object>> after = new HashMap<>();

    public void putBefore(String objectName, String attributeName, Object attributeValue)
    {
        before.computeIfAbsent(objectName, k -> new HashMap<>());
        before.get(objectName).put(attributeName, attributeValue);
    }

    public void putAfter(String objectName, String attributeName, Object attributeValue)
    {
        after.computeIfAbsent(objectName, k -> new HashMap<>());
        after.get(objectName).put(attributeName, attributeValue);
    }

    public Map<String, Map<String, Object>> getBefore()
    {
        return before;
    }

    public Map<String, Map<String, Object>> getAfter()
    {
        return after;
    }
}
