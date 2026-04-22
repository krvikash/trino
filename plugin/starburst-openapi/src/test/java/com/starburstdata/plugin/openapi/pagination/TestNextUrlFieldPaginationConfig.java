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

final class TestNextUrlFieldPaginationConfig
{
    @Test
    void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(NextUrlFieldPaginationConfig.class)
                .setNextUrlFieldJsonPointer(null));
    }

    @Test
    void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("openapi.pagination.next-url-cursor.next-url-field-json-pointer", "/paging/next")
                .buildOrThrow();

        NextUrlFieldPaginationConfig expected = new NextUrlFieldPaginationConfig()
                .setNextUrlFieldJsonPointer("/paging/next");

        assertFullMapping(properties, expected);
    }

    @Test
    void testNextUrlFieldValidation()
    {
        assertFailsValidation(
                new NextUrlFieldPaginationConfig(),
                "nextUrlFieldJsonPointer",
                "must not be null",
                NotNull.class);
    }
}
