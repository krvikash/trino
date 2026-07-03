/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.substitution;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.starburst.server.substitution.MaterializedViewSubstitutionConfig.MaterializationMetastoreType.IN_MEMORY;
import static io.starburst.server.substitution.MaterializedViewSubstitutionConfig.MaterializationMetastoreType.REST;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestMaterializedViewSubstitutionConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(MaterializedViewSubstitutionConfig.class)
                .setMaterializedViewSubstitutionSupportEnabled(false)
                .setMaterializedViewSubstitutionEnabled(false)
                .setMaterializedViewSubstitutionMetastoreRefreshInterval(new Duration(1, MINUTES))
                .setMaterializationMetastoreType(IN_MEMORY)
                .setMaterializedViewSubstitutionMaxStaleness(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("materialized-view-substitution.support.enabled", "true")
                .put("materialized-view-substitution.enabled", "true")
                .put("materialized-view-substitution.metastore-refresh-interval", "30s")
                .put("materialization.metastore.type", "REST")
                .put("materialized-view-substitution.max-staleness", "5m")
                .buildOrThrow();

        MaterializedViewSubstitutionConfig expected = new MaterializedViewSubstitutionConfig()
                .setMaterializedViewSubstitutionSupportEnabled(true)
                .setMaterializedViewSubstitutionEnabled(true)
                .setMaterializedViewSubstitutionMetastoreRefreshInterval(new Duration(30, SECONDS))
                .setMaterializationMetastoreType(REST)
                .setMaterializedViewSubstitutionMaxStaleness(new Duration(5, TimeUnit.MINUTES));

        assertFullMapping(properties, expected);
    }
}
