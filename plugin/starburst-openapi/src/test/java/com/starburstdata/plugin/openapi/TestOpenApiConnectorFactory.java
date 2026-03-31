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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.CreationException;
import com.google.inject.spi.Message;
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
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.starburstdata.plugin.openapi.OpenApiSpec.SCHEMA_NAME;
import static io.trino.spi.StandardErrorCode.CONFIGURATION_INVALID;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
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
        List<Exception> exceptions = assertThat(getConfigurationThrowable("ambiguouspaths.json"))
                .cause()
                .asInstanceOf(type(OpenApiValidationExceptions.class))
                .extracting(OpenApiValidationExceptions::getSpecificationExceptions)
                .actual();

        assertThat(exceptions).hasSize(2);
        assertThat(exceptions)
                .map(Exception::getMessage)
                .anySatisfy(message ->
                        assertThat(message)
                                .startsWith("Identifier colliding_path maps to multiple API paths"));
        assertThat(exceptions)
                .map(Exception::getMessage)
                .anySatisfy(message ->
                        assertThat(message)
                                .startsWith("Identifier non_unique maps to multiple API paths"));
    }

    @Test
    public void testResponses()
    {
        List<Exception> exceptions = assertThat(getConfigurationThrowable("responses.json"))
                .cause()
                .asInstanceOf(type(OpenApiValidationExceptions.class))
                .extracting(OpenApiValidationExceptions::getSpecificationExceptions)
                .actual();

        assertThat(exceptions)
                .map(Exception::getMessage)
                .containsExactlyInAnyOrderElementsOf(ImmutableList.<String>builder()
                        .add("Failed to transform path /circular (Reference from response forms a cycle)")
                        .add("Failed to transform path /badref (Reference refers to response 'badref' that doesn't exist)")
                        .build());
    }

    @Test
    public void testPaths()
    {
        List<Exception> exceptions = assertThat(getConfigurationThrowable("paths.json"))
                .cause()
                .asInstanceOf(type(OpenApiValidationExceptions.class))
                .extracting(OpenApiValidationExceptions::getSpecificationExceptions)
                .actual();

        assertThat(exceptions)
                .map(Exception::getMessage)
                .containsExactlyInAnyOrderElementsOf(ImmutableList.<String>builder()
                        .add("Failed to transform path /circular (Reference from path forms a cycle)")
                        .add("Failed to transform path /circularSTART (Reference from path forms a cycle)")
                        .add("Failed to transform path /circularEND (Reference from path forms a cycle)")
                        .add("Failed to transform path /badref (Reference refers to path 'notreal' that doesn't exist)")
                        .build());
    }

    @Test
    public void testAmbiguousObject()
    {
        List<Exception> exceptions = assertThat(getConfigurationThrowable("ambiguousobject.json"))
                .cause()
                .asInstanceOf(type(OpenApiValidationExceptions.class))
                .extracting(OpenApiValidationExceptions::getSpecificationExceptions)
                .actual();

        assertThat(exceptions)
                .map(Exception::getMessage)
                .containsExactly("Failed to transform path /ambiguousobject (properties: Uses keys that cannot be referenced unambiguously with case-insensitivity: AMBIGUOUS)");
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
