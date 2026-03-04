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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.STRING;

final class TestOpenApiWithFastApiServer
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        FastApiServer fastApiServer = closeAfterClass(new FastApiServer());

        return OpenApiQueryRunner.builder()
                .addConnectorProperties(ImmutableMap.<String, String>builder()
                        .put("openapi.spec-location", fastApiServer.getSpecUrl())
                        .put("openapi.base-uri", fastApiServer.getApiUrl()).buildOrThrow())
                .build();
    }

    @Test
    void testStubsFunctions()
    {
        assertThat(query("SELECT * FROM TABLE(openapi.default.item_categories())"))
                .result()
                .onlyColumnAsSet()
                .singleElement()
                .asInstanceOf(STRING)
                .contains("Portal Gun");
    }
}
