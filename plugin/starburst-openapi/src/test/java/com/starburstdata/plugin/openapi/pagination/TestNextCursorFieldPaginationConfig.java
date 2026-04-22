/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.plugin.openapi.pagination;

import com.google.common.collect.ImmutableMap;
import jakarta.validation.constraints.NotNull;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.testing.ValidationAssertions.assertFailsValidation;

final class TestNextCursorFieldPaginationConfig
{
    @Test
    void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(NextCursorFieldPaginationConfig.class)
                .setCursorParameterName(null)
                .setCursorFieldJsonPointer(null));
    }

    @Test
    void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("openapi.pagination.next-field-cursor.cursor-parameter-name", "cursor")
                .put("openapi.pagination.next-field-cursor.cursor-field-json-pointer", "/response_metadata/next_cursor")
                .buildOrThrow();

        NextCursorFieldPaginationConfig expected = new NextCursorFieldPaginationConfig()
                .setCursorParameterName("cursor")
                .setCursorFieldJsonPointer("/response_metadata/next_cursor");

        assertFullMapping(properties, expected);
    }

    @Test
    void testCursorParamValidation()
    {
        assertFailsValidation(
                new NextCursorFieldPaginationConfig()
                        .setCursorFieldJsonPointer("/response_metadata/next_cursor")
                        .setCursorParameterName(null),
                "cursorParameterName",
                "must not be null",
                NotNull.class);
    }

    @Test
    void testNextCursorFieldValidation()
    {
        assertFailsValidation(
                new NextCursorFieldPaginationConfig(),
                "cursorFieldJsonPointer",
                "must not be null",
                NotNull.class);
    }
}
