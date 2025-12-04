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
package com.starburstdata.plugin.openapi;

import com.google.common.collect.ImmutableMap;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

final class TestOpenApiWithOpenMeteo
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return OpenApiQueryRunner.builder()
                .addConnectorProperties(ImmutableMap.<String, String>builder()
                        .put("openapi.spec-location", "https://raw.githubusercontent.com/open-meteo/open-meteo/main/openapi.yml")
                        .put("openapi.base-uri", "https://api.open-meteo.com")
                        .buildOrThrow())
                .build();
    }

    @Test
    void testSelectFromForecastTable()
    {
        assertQuery("SELECT elevation, timezone, current_weather.temperature BETWEEN -50 AND 100 AS is_livable " +
                        "FROM openapi.default.v1_forecast WHERE latitude_req = 53.1325 AND longitude_req = 23.1688",
                "VALUES (135.0, 'GMT', null)");
        assertQuery("SELECT elevation, timezone, current_weather.temperature BETWEEN -50 AND 100 AS is_livable " +
                        "FROM openapi.default.v1_forecast WHERE latitude_req = 53.1325 AND longitude_req = 23.1688 AND current_weather_req = true",
                "VALUES (135.0, 'GMT', true)");
    }
}
