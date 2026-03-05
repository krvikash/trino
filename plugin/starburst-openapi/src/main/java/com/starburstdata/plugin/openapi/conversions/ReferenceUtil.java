/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.plugin.openapi.conversions;

import com.fasterxml.jackson.core.JsonPointer;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;

import java.net.URI;
import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.lang.String.join;

public final class ReferenceUtil
{
    private ReferenceUtil()
    {}

    public static String extractRefKey(List<String> prefix, String ref)
    {
        URI uri = URI.create(ref);
        String fragment = extractFragment(uri);
        JsonPointer jsonPointer = JsonPointer.compile(fragment);
        return extractComponentKey(prefix, jsonPointer);
    }

    private static String extractFragment(URI uri)
    {
        // Only support #/components/schemas/[key] form, no outbound requests.
        // https://docs.oracle.com/en/java/javase/25/docs//api/java.base/java/net/URI.html
        // > At the highest level a URI reference (hereinafter simply "URI") in string form has the syntax
        // > [scheme:]scheme-specific-part[#fragment]
        if (uri.getScheme() != null && !uri.getScheme().isEmpty()) {
            throw new IllegalArgumentException(
                    format(
                            "Ref was not a local fragment, had scheme %s: %s",
                            uri.getScheme(),
                            uri));
        }
        if (!uri.getSchemeSpecificPart().isEmpty()) {
            throw new IllegalArgumentException(format(
                    "Ref was not a local fragment, had scheme specific part %s: %s",
                    uri.getSchemeSpecificPart(),
                    uri));
        }
        String fragment = uri.getFragment();
        if (fragment == null) {
            throw new IllegalArgumentException(format(
                    "Ref did not have a local fragment: %s",
                    uri));
        }
        return fragment;
    }

    private static String extractComponentKey(List<String> prefix, JsonPointer pointer)
    {
        List<String> properties = ImmutableList.copyOf(getMatchingProperties(pointer));
        checkArgument(
                properties.size() == prefix.size() + 1,
                "Expected JSON pointer to access exactly %d properties".formatted(prefix.size() + 1));
        checkArgument(
                properties.subList(0, prefix.size()).equals(prefix),
                "Expected JSON pointer to have prefix formed by properties: %s".formatted(join(", ", prefix)));
        return properties.get(prefix.size());
    }

    private static Iterator<String> getMatchingProperties(JsonPointer pointer)
    {
        return new AbstractIterator<>()
        {
            private JsonPointer mutablePointer = pointer;

            @Override
            protected String computeNext()
            {
                if (mutablePointer.matches()) {
                    return endOfData();
                }
                String matchingProperty = mutablePointer.getMatchingProperty();
                mutablePointer = mutablePointer.tail();
                return matchingProperty;
            }
        };
    }
}
