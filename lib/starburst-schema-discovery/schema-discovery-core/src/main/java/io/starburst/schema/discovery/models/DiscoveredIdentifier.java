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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import static io.starburst.schema.discovery.models.HiveIdentifier.toHiveIdentifier;

public sealed interface DiscoveredIdentifier
        permits HiveIdentifier, TrinoIdentifier
{
    String string();

    @JsonCreator
    static DiscoveredIdentifier of(String value)
    {
        // when deserializing, we can use VALID_IN_TRINO, as it covers all values
        return TrinoIdentifier.toTrinoIdentifier(value);
    }

    static DiscoveredIdentifier toTrinoIdentifier(LowerCaseString value)
    {
        return new TrinoIdentifier(value);
    }

    // jackson does not like @JsonValue directly on interface method that delegates to package-private record
    @JsonValue
    default String toJsonValue()
    {
        return string();
    }

    static DiscoveredIdentifier identifierFromString(String rawIdentifier, IdentifierConstraint constraint)
    {
        return switch (constraint) {
            case VALID_IN_TRINO -> TrinoIdentifier.toTrinoIdentifier(rawIdentifier);
            case VALID_IN_HIVE_AND_TRINO -> toHiveIdentifier(rawIdentifier);
        };
    }
}
