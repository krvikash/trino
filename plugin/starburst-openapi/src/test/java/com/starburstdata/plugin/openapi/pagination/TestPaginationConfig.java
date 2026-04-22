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

import static com.starburstdata.plugin.openapi.pagination.PaginationConfig.PaginationType.NEXT_CURSOR_FIELD;
import static com.starburstdata.plugin.openapi.pagination.PaginationConfig.PaginationType.NONE;
import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.testing.ValidationAssertions.assertFailsValidation;

final class TestPaginationConfig
{
    @Test
    void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(PaginationConfig.class)
                .setPaginationType(NONE));
    }

    @Test
    void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("openapi.pagination", "NEXT_CURSOR_FIELD")
                .buildOrThrow();

        PaginationConfig expected = new PaginationConfig()
                .setPaginationType(NEXT_CURSOR_FIELD);

        assertFullMapping(properties, expected);
    }

    @Test
    void testPaginationStrategyTypeValidation()
    {
        assertFailsValidation(
                new PaginationConfig()
                        .setPaginationType(null),
                "paginationType",
                "must not be null",
                NotNull.class);
    }
}
