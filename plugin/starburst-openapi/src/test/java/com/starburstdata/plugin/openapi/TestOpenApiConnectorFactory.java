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
import com.google.inject.CreationException;
import com.google.inject.spi.Message;
import com.starburstdata.plugin.openapi.OpenApiValidationExceptions.AmbiguousTableFunctionPath;
import com.starburstdata.plugin.openapi.OpenApiValidationExceptions.BadPathItem;
import com.starburstdata.plugin.openapi.OpenApiValidationExceptions.BadResponseReference;
import com.starburstdata.plugin.openapi.OpenApiValidationExceptions.FailedValidation;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.Connector;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.spi.function.table.ReturnTypeSpecification.DescribedTable;
import io.trino.testing.TestingConnectorContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.URL;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.starburstdata.plugin.openapi.OpenApiSpec.SCHEMA_NAME;
import static io.trino.spi.StandardErrorCode.CONFIGURATION_INVALID;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.assertj.core.api.Fail.fail;
import static org.assertj.core.api.InstanceOfAssertFactories.list;
import static org.assertj.core.api.InstanceOfAssertFactories.throwable;
import static org.assertj.core.api.InstanceOfAssertFactories.type;

final class TestOpenApiConnectorFactory
{
    @ParameterizedTest
    @ValueSource(strings = {
            "github.json",
            "github-patched.json",
            "jira.json",
            "galaxy.json",
            "petstore.yaml",
            "datadog.yaml",
            "cloudflare.json",
            "openmeteo.yml"
    })
    public void testLoadsSpecification(String specification)
    {
        assertThatNoException().isThrownBy(() -> createConnector(specification).shutdown());
    }

    @Test
    void testTableFunctions()
    {
        Connector connector = createConnector("petstore.yaml");
        assertThat(connector.getTableFunctions())
                .extracting(ConnectorTableFunction::getSchema)
                .containsOnly(SCHEMA_NAME);
        assertThat(connector.getTableFunctions())
                .extracting(ConnectorTableFunction::getName)
                .containsExactlyInAnyOrder(
                        "pet_find_by_status",
                        "pet_find_by_tags",
                        "pet_pet_id",
                        "store_inventory",
                        "store_order_order_id",
                        "user_login",
                        "user_username");
        assertThat(connector.getTableFunctions())
                .extracting(ConnectorTableFunction::getReturnTypeSpecification)
                .allMatch(rt -> rt instanceof DescribedTable);
        connector.shutdown();
    }

    @Test
    void testAmbiguousTableFunctions()
    {
        assertThat(getConfigurationThrowable("ambiguouspaths.json"))
                .cause()
                .asInstanceOf(type(OpenApiValidationExceptions.class))
                .extracting(OpenApiValidationExceptions::getFailedValidations)
                .asInstanceOf(list(FailedValidation.class))
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

    @Test
    public void testResponses()
    {
        Map<String, String> badResponseReferences =
                assertThat(getConfigurationThrowable("responses.json"))
                .cause()
                .asInstanceOf(type(OpenApiValidationExceptions.class))
                .extracting(OpenApiValidationExceptions::getFailedValidations)
                .asInstanceOf(list(FailedValidation.class))
                .asInstanceOf(list(BadResponseReference.class))
                .actual()
                .stream()
                .collect(toImmutableMap(
                        BadResponseReference::path,
                        BadResponseReference::error));
        assertThat(badResponseReferences).containsOnlyKeys("/circular", "/badref");
        assertThat(badResponseReferences.get("/circular"))
                .isEqualTo("Response references form a cycle");
        assertThat(badResponseReferences.get("/badref"))
                .isEqualTo("Response references re-usable response that doesn't exist: badref");
    }

    @Test
    public void testPaths()
    {
        Map<String, String> badPaths = assertThat(getConfigurationThrowable("paths.json"))
                .cause()
                .asInstanceOf(type(OpenApiValidationExceptions.class))
                .extracting(OpenApiValidationExceptions::getFailedValidations)
                .asInstanceOf(list(FailedValidation.class))
                .asInstanceOf(list(BadPathItem.class))
                .actual()
                .stream()
                .collect(toImmutableMap(
                        BadPathItem::path,
                        BadPathItem::error));
        assertThat(badPaths).containsOnlyKeys("/circular", "/circularSTART", "/circularEND", "/badref");
        assertThat(badPaths.get("/circular"))
                .isEqualTo("Path references form a cycle");
        assertThat(badPaths.get("/circularSTART"))
                .isEqualTo("Path references form a cycle");
        assertThat(badPaths.get("/badref"))
                .isEqualTo("Path references path that doesn't exist: notreal");
    }

    private Connector createConnector(String location)
    {
        URL specResource = requireNonNull(getClass().getClassLoader().getResource(location));
        Map<String, String> config = ImmutableMap.<String, String>builder()
                .put("bootstrap.quiet", "true")
                .put("openapi.spec-location", specResource.getFile())
                .put("openapi.base-uri", "https://starburst.io")
                .buildOrThrow();
        return new OpenApiConnectorFactory().create("openapi", config, new TestingConnectorContext());
    }

    private Throwable getConfigurationThrowable(String location)
    {
        Collection<Message> creationMessages = assertThatThrownBy(() -> createConnector(location))
                .asInstanceOf(throwable(CreationException.class))
                .extracting(CreationException::getErrorMessages)
                .actual();
        Optional<Throwable> exception = creationMessages.stream()
                .flatMap(e -> Optional.ofNullable(e.getCause()).stream())
                .filter(e -> e instanceof TrinoException trinoException &&
                        trinoException.getErrorCode().equals(CONFIGURATION_INVALID.toErrorCode()))
                .findFirst();
        return assertThat(exception).isPresent().get().actual();
    }
}
