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

final class TestOffsetPaginationConfig
{
    @Test
    void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(OffsetPaginationConfig.class)
                .setOffsetParameterName(null)
                .setDataFieldJsonPointer(null));
    }

    @Test
    void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("openapi.pagination.offset.offset-parameter-name", "offset")
                .put("openapi.pagination.offset.data-field-json-pointer", "/data")
                .buildOrThrow();

        OffsetPaginationConfig expected = new OffsetPaginationConfig()
                .setOffsetParameterName("offset")
                .setDataFieldJsonPointer("/data");

        assertFullMapping(properties, expected);
    }

    @Test
    void testOffsetParameterNameValidation()
    {
        assertFailsValidation(
                new OffsetPaginationConfig()
                        .setDataFieldJsonPointer("/data")
                        .setOffsetParameterName(null),
                "offsetParameterName",
                "must not be null",
                NotNull.class);
    }
}
