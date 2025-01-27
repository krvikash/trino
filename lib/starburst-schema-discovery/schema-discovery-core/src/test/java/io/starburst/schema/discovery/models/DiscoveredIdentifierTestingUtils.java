/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.models;

import static io.starburst.schema.discovery.models.LowerCaseString.toLowerCase;

public class DiscoveredIdentifierTestingUtils
{
    private DiscoveredIdentifierTestingUtils() {}

    public static DiscoveredIdentifier toTestingHiveIdentifier(String rawIdentifier)
    {
        return HiveIdentifier.toHiveIdentifier(rawIdentifier);
    }

    public static DiscoveredIdentifier toTestingIdentifier(String rawIdentifier)
    {
        return toTestingIdentifier(toLowerCase(rawIdentifier));
    }

    public static DiscoveredIdentifier toTestingIdentifier(LowerCaseString lowerCaseIdentifier)
    {
        return TrinoIdentifier.toTrinoIdentifier(lowerCaseIdentifier.getOriginalString());
    }
}
