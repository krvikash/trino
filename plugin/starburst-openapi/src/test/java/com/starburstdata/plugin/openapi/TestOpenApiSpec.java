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

import com.starburstdata.plugin.openapi.OpenApiValidationExceptions.AmbiguousTableFunctionPath;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.spi.function.table.ReturnTypeSpecification.DescribedTable;
import org.junit.jupiter.api.Test;

import java.net.URL;

import static com.starburstdata.plugin.openapi.OpenApiSpec.SCHEMA_NAME;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Fail.fail;
import static org.assertj.core.api.InstanceOfAssertFactories.list;
import static org.assertj.core.api.InstanceOfAssertFactories.type;

final class TestOpenApiSpec
{
    @Test
    public void testLoadsGithub()
    {
        assertThatNoException().isThrownBy(() -> loadSpec("github.json"));
    }

    @Test
    public void testLoadsGithubPatched()
    {
        assertThatNoException().isThrownBy(() -> loadSpec("github-patched.json"));
    }

    @Test
    public void testLoadsJira()
    {
        assertThatNoException().isThrownBy(() -> loadSpec("jira.json"));
    }

    @Test
    public void testLoadsGalaxy()
    {
        assertThatNoException().isThrownBy(() -> loadSpec("galaxy.json"));
    }

    @Test
    public void testLoadsPetstore()
    {
        assertThatNoException().isThrownBy(() -> loadSpec("petstore.yaml"));
    }

    @Test
    public void testLoadsDatadog()
    {
        assertThatNoException().isThrownBy(() -> loadSpec("datadog.yaml"));
    }

    @Test
    public void testLoadsCloudflare()
    {
        assertThatNoException().isThrownBy(() -> loadSpec("cloudflare.json"));
    }

    @Test
    public void testLoadsOpenMeteo()
    {
        assertThatNoException().isThrownBy(() -> loadSpec("openmeteo.yml"));
    }

    @Test
    void testTableFunctions()
    {
        OpenApiSpec spec = loadSpec("petstore.yaml");
        assertThat(spec.getTableFunctions())
                .extracting(ConnectorTableFunction::getSchema)
                .containsOnly(SCHEMA_NAME);
        assertThat(spec.getTableFunctions())
                .extracting(ConnectorTableFunction::getName)
                .containsExactlyInAnyOrder(
                        "pet_find_by_status",
                        "pet_find_by_tags",
                        "pet_pet_id",
                        "store_inventory",
                        "store_order_order_id",
                        "user_login",
                        "user_username");
        assertThat(spec.getTableFunctions())
                .extracting(ConnectorTableFunction::getReturnTypeSpecification)
                .allMatch(rt -> rt instanceof DescribedTable);
    }

    @Test
    void testAmbiguousTableFunctions()
    {
        assertTrinoExceptionThrownBy(() -> loadSpec("ambiguouspaths.json"))
                .hasErrorCode(StandardErrorCode.CONFIGURATION_INVALID)
                .cause()
                .asInstanceOf(type(OpenApiValidationExceptions.class))
                .extracting(OpenApiValidationExceptions::getFailedValidations)
                .asInstanceOf(list(OpenApiValidationExceptions.FailedValidation.class))
                .hasOnlyElementsOfType(AmbiguousTableFunctionPath.class)
                .asInstanceOf(list(AmbiguousTableFunctionPath.class))
                .allSatisfy(ambiguousError -> {
                    switch (ambiguousError.identifier()) {
                        case "colliding_path" -> assertThat(ambiguousError)
                                .extracting(AmbiguousTableFunctionPath::paths)
                                .asInstanceOf(list(String.class))
                                .containsExactlyInAnyOrder("/collidingPath", "/colliding_path");
                        case "non_unique" -> assertThat(ambiguousError)
                                .extracting(AmbiguousTableFunctionPath::paths)
                                .asInstanceOf(list(String.class))
                                .containsExactlyInAnyOrder("/non/unique", "/non/{unique}");
                        case String other -> fail("Unexpected ambiguous identifier %s", other);
                    }
                });
    }

    private OpenApiSpec loadSpec(String name)
    {
        OpenApiConfig config = new OpenApiConfig();
        URL specResource = requireNonNull(getClass().getClassLoader().getResource(name));
        config.setSpecLocation(specResource.getFile());
        return new OpenApiSpec(config);
    }
}
