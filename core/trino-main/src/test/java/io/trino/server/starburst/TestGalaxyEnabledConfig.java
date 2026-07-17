/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.server.starburst;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestGalaxyEnabledConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(GalaxyEnabledConfig.class)
                .setGalaxyEnabled(false)
                .setGalaxyRbacEnabled(true)
                .setGalaxyCorsEnabled(false)
                .setGalaxyOperatorAuthenticationEnabled(false)
                .setGalaxyHeartbeatEnabled(false)
                .setGalaxyTransactionsDisabled(false)
                .setUseSharedPermissionsCache(false));
    }

    @Test
    public void testExplicitPropertyMappings()
            throws IOException
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("galaxy.enabled", "true")
                .put("galaxy.rbac.enabled", "true")
                .put("galaxy.cors.enabled", "true")
                .put("galaxy.heartbeat.enabled", "true")
                .put("galaxy.operator.enabled", "true")
                .put("galaxy.transaction.disabled", "true")
                .put("galaxy.use-shared-permissions-cache", "true")
                .buildOrThrow();

        GalaxyEnabledConfig expected = new GalaxyEnabledConfig()
                .setGalaxyEnabled(true)
                .setGalaxyRbacEnabled(true)
                .setGalaxyCorsEnabled(true)
                .setGalaxyOperatorAuthenticationEnabled(true)
                .setGalaxyHeartbeatEnabled(true)
                .setGalaxyTransactionsDisabled(true)
                .setUseSharedPermissionsCache(true);

        assertFullMapping(properties, expected);
    }
}
