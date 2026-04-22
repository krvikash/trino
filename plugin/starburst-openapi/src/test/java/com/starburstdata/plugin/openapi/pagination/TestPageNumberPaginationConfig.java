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

final class TestPageNumberPaginationConfig
{
    @Test
    void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(PageNumberPaginationConfig.class)
                .setPageParameterName(null)
                .setIsLastPageFieldJsonPointer(null));
    }

    @Test
    void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("openapi.pagination.page-number.page-parameter-name", "page")
                .put("openapi.pagination.page-number.is-last-page-field-json-pointer", "/last")
                .buildOrThrow();

        PageNumberPaginationConfig expected = new PageNumberPaginationConfig()
                .setPageParameterName("page")
                .setIsLastPageFieldJsonPointer("/last");

        assertFullMapping(properties, expected);
    }

    @Test
    void testPageParamValidation()
    {
        assertFailsValidation(
                new PageNumberPaginationConfig()
                        .setPageParameterName(null),
                "pageParameterName",
                "must not be null",
                NotNull.class);
    }

    @Test
    void testIsLastPageFieldJsonPointerValidation()
    {
        assertFailsValidation(
                new PageNumberPaginationConfig()
                        .setPageParameterName("page")
                        .setIsLastPageFieldJsonPointer(null),
                "isLastPageFieldJsonPointer",
                "must not be null",
                NotNull.class);
    }
}
