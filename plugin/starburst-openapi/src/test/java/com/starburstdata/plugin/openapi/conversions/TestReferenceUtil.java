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

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;

import static com.starburstdata.plugin.openapi.conversions.ReferenceUtil.extractRefKey;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestReferenceUtil
{
    @Test
    public void testExtractValidKey()
    {
        assertThat(extractRefKey(ImmutableList.of("components", "schemas"), "#/components/schemas/shared_schema"))
                .isEqualTo("shared_schema");
        assertThat(extractRefKey(ImmutableList.of("components", "parameters"), "#/components/parameters/param"))
                .isEqualTo("param");
        assertThat(extractRefKey(ImmutableList.of("paths"), "#/paths/my~0path~1"))
                .isEqualTo("my~path/");
    }

    @Test
    public void testResolveInvalidURI()
    {
        assertThatThrownBy(() -> extractRefKey(ImmutableList.of(), "♤://invalid_authority.com"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasCauseInstanceOf(URISyntaxException.class);
    }

    @Test
    public void testResolveNonLocalFragments()
    {
        assertThatThrownBy(() -> extractRefKey(ImmutableList.of(), "scheme://_"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("Ref was not a local fragment");

        assertThatThrownBy(() -> extractRefKey(ImmutableList.of(), "host#fragment"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("Ref was not a local fragment");

        assertThatThrownBy(() -> extractRefKey(ImmutableList.of(), ""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("Ref did not have a local fragment");
    }

    @Test
    public void testResolveIllegalJsonPointerFragment()
    {
        assertThatThrownBy(() -> extractRefKey(ImmutableList.of(), "#~3"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("Invalid input: JSON Pointer expression must start with '/'");
    }

    @Test
    public void testResolveBadJsonPointerFragment()
    {
        assertThatThrownBy(() -> extractRefKey(ImmutableList.of("components", "schemas"), "#/not_components/schemas/key"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Expected JSON pointer to have prefix formed by properties: components, schemas");

        assertThatThrownBy(() -> extractRefKey(ImmutableList.of("components", "schemas"), "#/components/non_schemas/key"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Expected JSON pointer to have prefix formed by properties: components, schemas");

        assertThatThrownBy(() -> extractRefKey(ImmutableList.of("components", "schemas"), "#/components"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Expected JSON pointer to access exactly 3 properties");

        assertThatThrownBy(() -> extractRefKey(ImmutableList.of("components", "schemas"), "#/components/"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Expected JSON pointer to access exactly 3 properties");

        assertThatThrownBy(() -> extractRefKey(ImmutableList.of("components", "schemas"), "#/components/schemas"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Expected JSON pointer to access exactly 3 properties");

        assertThatThrownBy(() -> extractRefKey(ImmutableList.of("components", "schemas"), "#/components/schemas/extra/depth"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Expected JSON pointer to access exactly 3 properties");
    }
}
