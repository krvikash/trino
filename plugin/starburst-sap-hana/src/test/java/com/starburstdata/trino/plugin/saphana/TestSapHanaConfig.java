/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.saphana;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.starburstdata.trino.plugin.saphana.SapHanaParallelismType.NO_PARALLELISM;
import static com.starburstdata.trino.plugin.saphana.SapHanaParallelismType.PARTITIONS;
import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

final class TestSapHanaConfig
{
    @Test
    void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(SapHanaConfig.class)
                .setParallelismType(NO_PARALLELISM));
    }

    @Test
    void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("sap-hana.parallelism-type", "PARTITIONS")
                .buildOrThrow();

        SapHanaConfig expected = new SapHanaConfig()
                .setParallelismType(PARTITIONS);

        assertFullMapping(properties, expected);
    }
}
