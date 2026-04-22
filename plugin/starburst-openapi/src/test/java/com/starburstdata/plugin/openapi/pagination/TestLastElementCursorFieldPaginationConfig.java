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

final class TestLastElementCursorFieldPaginationConfig
{
    @Test
    void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(LastElementCursorFieldPaginationConfig.class)
                .setCursorParameterName(null)
                .setDataFieldJsonPointer(null)
                .setCursorFieldJsonPointer(null));
    }

    @Test
    void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("openapi.pagination.last-element-cursor-field.cursor-parameter-name", "starting_after")
                .put("openapi.pagination.last-element-cursor-field.data-field-json-pointer", "/data")
                .put("openapi.pagination.last-element-cursor-field.cursor-field-json-pointer", "/id")
                .buildOrThrow();

        LastElementCursorFieldPaginationConfig expected = new LastElementCursorFieldPaginationConfig()
                .setCursorParameterName("starting_after")
                .setDataFieldJsonPointer("/data")
                .setCursorFieldJsonPointer("/id");

        assertFullMapping(properties, expected);
    }

    @Test
    void testCursorParamValidation()
    {
        assertFailsValidation(
                new LastElementCursorFieldPaginationConfig()
                        .setDataFieldJsonPointer("/data")
                        .setCursorFieldJsonPointer("/id")
                        .setCursorParameterName(null),
                "cursorParameterName",
                "must not be null",
                NotNull.class);
    }

    @Test
    void testCursorElementFieldValidation()
    {
        assertFailsValidation(
                new LastElementCursorFieldPaginationConfig()
                        .setDataFieldJsonPointer("/data"),
                "cursorFieldJsonPointer",
                "must not be null",
                NotNull.class);
    }
}
