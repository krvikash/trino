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
import com.google.common.collect.ImmutableSet;
import com.google.inject.CreationException;
import com.google.inject.spi.Message;
import com.starburstdata.plugin.openapi.OpenApiConfig.CastPolicy;
import io.airlift.bootstrap.ApplicationConfigurationException;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.spi.function.table.ReturnTypeSpecification.DescribedTable;
import io.trino.testing.TestingConnectorContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.net.URL;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.starburstdata.plugin.openapi.OpenApiConfig.CastPolicy.DROP;
import static com.starburstdata.plugin.openapi.OpenApiDescription.SCHEMA_NAME;
import static io.trino.spi.StandardErrorCode.CONFIGURATION_INVALID;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.set;
import static org.assertj.core.api.InstanceOfAssertFactories.throwable;
import static org.assertj.core.api.InstanceOfAssertFactories.type;

final class TestOpenApiConnectorFactory
{
    private static final List<String> FULLY_SUPPORTED_SPECS = ImmutableList.of(
            "galaxy.json",
            "petstore.yaml",
            "openmeteo.yml");

    private static final List<String> PARTIALLY_SUPPORTED_SPECS = ImmutableList.of(
            "github.json",
            "github-patched.json",
            "jira.json",
            "datadog.yaml");

    @ParameterizedTest
    @MethodSource("fullySupportedSpecs")
    public void testFullySupportedSpec(String description)
    {
        assertThatNoException().isThrownBy(() -> createConnector(description).shutdown());
    }

    @ParameterizedTest
    @MethodSource("partiallySupportedSpecs")
    public void testPartiallySupportedSpecFailsByDefault(String description)
    {
        // Fail from unsupported parameters with default ERROR policy.
        assertThat(getConfigurationThrowable(description))
                .cause()
                .isInstanceOf(OpenApiValidationExceptions.class);
    }

    @ParameterizedTest
    @MethodSource("allSpecsWithPermissivePolicy")
    public void testAllSpecsLoadWithPermissivePolicy(String description, CastPolicy castPolicy)
    {
        assertThatNoException().isThrownBy(() -> createConnector(description, castPolicy).shutdown());
    }

    private static Stream<String> fullySupportedSpecs()
    {
        return FULLY_SUPPORTED_SPECS.stream();
    }

    private static Stream<String> partiallySupportedSpecs()
    {
        return PARTIALLY_SUPPORTED_SPECS.stream();
    }

    private static Stream<Arguments> allSpecsWithPermissivePolicy()
    {
        List<String> allSpecs = ImmutableList.<String>builder()
                .addAll(FULLY_SUPPORTED_SPECS)
                .addAll(PARTIALLY_SUPPORTED_SPECS)
                .build();
        return Stream.of(DROP, CastPolicy.FALLBACK)
                .flatMap(policy -> allSpecs.stream().map(spec -> Arguments.of(spec, policy)));
    }

    @Test
    public void testCloudflareFails()
    {
        // Fail from ambiguous references.
        assertThat(getConfigurationThrowable("cloudflare.json"))
                .cause()
                .isInstanceOf(OpenApiValidationExceptions.class);
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
    void testSystemTableRegistered()
    {
        Connector connector = createConnector("petstore.yaml");
        assertThat(connector.getSystemTables())
                .extracting(SystemTable::getTableMetadata)
                .extracting(ConnectorTableMetadata::getTable)
                .containsExactly(new SchemaTableName("system", "table_functions"));
        connector.shutdown();
    }

    @Test
    void testAmbiguousTableFunctions()
    {
        // Ambiguous path identifiers are structural errors unaffected by FALLBACK.
        for (CastPolicy castPolicy : ImmutableList.of(CastPolicy.ERROR, CastPolicy.FALLBACK)) {
            List<Exception> exceptions = assertThat(getConfigurationThrowable("ambiguouspaths.json", castPolicy))
                    .cause()
                    .asInstanceOf(type(OpenApiValidationExceptions.class))
                    .extracting(OpenApiValidationExceptions::getDescriptionExceptions)
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
        // DROP logs the collision warnings and loads the connector anyway.
        assertThatNoException().isThrownBy(() -> createConnector("ambiguouspaths.json", DROP).shutdown());
    }

    @Test
    public void testResponses()
    {
        // Circular and missing references are structural errors unaffected by FALLBACK.
        for (CastPolicy castPolicy : ImmutableList.of(CastPolicy.ERROR, CastPolicy.FALLBACK)) {
            List<Exception> exceptions = assertThat(getConfigurationThrowable("responses.json", castPolicy))
                    .cause()
                    .asInstanceOf(type(OpenApiValidationExceptions.class))
                    .extracting(OpenApiValidationExceptions::getDescriptionExceptions)
                    .actual();

            assertThat(exceptions)
                    .map(Exception::getMessage)
                    .containsExactlyInAnyOrderElementsOf(ImmutableList.<String>builder()
                            .add("paths./circular.get.responses.200.$ref.circularSTART.$ref.circularEND.$ref: Reference from response forms a cycle")
                            .add("paths./badref.get.responses.200.$ref: Reference refers to response 'badref' that doesn't exist")
                            .build());
        }
        // DROP drops the unresolvable paths and loads the connector.
        assertThatNoException().isThrownBy(() -> createConnector("responses.json", DROP).shutdown());
    }

    @Test
    public void testPaths()
    {
        // Circular and missing path references are structural errors unaffected by FALLBACK.
        for (CastPolicy castPolicy : ImmutableList.of(CastPolicy.ERROR, CastPolicy.FALLBACK)) {
            List<Exception> exceptions = assertThat(getConfigurationThrowable("paths.json", castPolicy))
                    .cause()
                    .asInstanceOf(type(OpenApiValidationExceptions.class))
                    .extracting(OpenApiValidationExceptions::getDescriptionExceptions)
                    .actual();

            assertThat(exceptions)
                    .map(Exception::getMessage)
                    .containsExactlyInAnyOrderElementsOf(ImmutableList.<String>builder()
                            .add("paths./circular.$ref./circularSTART.$ref./circularEND.$ref: Reference from path forms a cycle")
                            .add("paths./circularSTART.$ref./circularEND.$ref./circularSTART.$ref: Reference from path forms a cycle")
                            .add("paths./circularEND.$ref./circularSTART.$ref./circularEND.$ref: Reference from path forms a cycle")
                            .add("paths./badref.$ref: Reference refers to path 'notreal' that doesn't exist")
                            .build());
        }
        // DROP drops the unresolvable paths and loads the connector.
        assertThatNoException().isThrownBy(() -> createConnector("paths.json", DROP).shutdown());
    }

    @Test
    public void testAmbiguousObject()
    {
        // Ambiguous property keys are structural errors unaffected by FALLBACK.
        for (CastPolicy castPolicy : ImmutableList.of(CastPolicy.ERROR, CastPolicy.FALLBACK)) {
            List<Exception> exceptions = assertThat(getConfigurationThrowable("ambiguousobject.json", castPolicy))
                    .cause()
                    .asInstanceOf(type(OpenApiValidationExceptions.class))
                    .extracting(OpenApiValidationExceptions::getDescriptionExceptions)
                    .actual();

            assertThat(exceptions)
                    .map(Exception::getMessage)
                    .containsExactly("paths./ambiguousobject.get.responses.200.content.application/json.schema.properties: Uses keys that cannot be referenced unambiguously with case-insensitivity: AMBIGUOUS");
        }
        // DROP drops the path with the ambiguous response schema and loads the connector.
        assertThatNoException().isThrownBy(() -> createConnector("ambiguousobject.json", DROP).shutdown());
    }

    @Test
    public void testParameters()
    {
        // All four errors reported with ERROR policy.
        List<Exception> errorExceptions = assertThat(getConfigurationThrowable("parameters.json", CastPolicy.ERROR))
                .cause()
                .asInstanceOf(type(OpenApiValidationExceptions.class))
                .extracting(OpenApiValidationExceptions::getDescriptionExceptions)
                .actual();

        assertThat(errorExceptions)
                .map(Exception::getMessage)
                .containsExactlyInAnyOrderElementsOf(ImmutableList.<String>builder()
                        .add("paths./badref.get.parameters[0].$ref: Reference refers to parameter 'badref' that doesn't exist")
                        .add("paths./circularref.get.parameters[0].$ref.circularSTART.$ref.circularEND.$ref: Reference from parameter forms a cycle")
                        .add("paths./badschema.get.parameters[0].schema: Schema uses unsupported boolean keywords [oneOf]")
                        .add("paths./ambiguous/{param}.get.parameters[1].name: Cannot refer to parameter 'param' unambiguously, parameter with identifier 'PARAM' already exists")
                        .build());

        // FALLBACK converts oneOf to JsonIr, so the badschema error disappears; structural errors remain.
        List<Exception> fallbackExceptions = assertThat(getConfigurationThrowable("parameters.json", CastPolicy.FALLBACK))
                .cause()
                .asInstanceOf(type(OpenApiValidationExceptions.class))
                .extracting(OpenApiValidationExceptions::getDescriptionExceptions)
                .actual();

        assertThat(fallbackExceptions)
                .map(Exception::getMessage)
                .containsExactlyInAnyOrderElementsOf(ImmutableList.<String>builder()
                        .add("paths./badref.get.parameters[0].$ref: Reference refers to parameter 'badref' that doesn't exist")
                        .add("paths./circularref.get.parameters[0].$ref.circularSTART.$ref.circularEND.$ref: Reference from parameter forms a cycle")
                        .add("paths./ambiguous/{param}.get.parameters[1].name: Cannot refer to parameter 'param' unambiguously, parameter with identifier 'PARAM' already exists")
                        .build());

        // DROP drops all paths with unresolvable parameters and loads the connector.
        assertThatNoException().isThrownBy(() -> createConnector("parameters.json", DROP).shutdown());
    }

    private Connector createConnector(String location)
    {
        return createConnector(location, CastPolicy.ERROR);
    }

    private Connector createConnector(String location, CastPolicy castPolicy)
    {
        URL descriptionResource = requireNonNull(getClass().getClassLoader().getResource(location));
        Map<String, String> config = ImmutableMap.<String, String>builder()
                .put("bootstrap.quiet", "true")
                .put("openapi.description-location", descriptionResource.getFile())
                .put("openapi.base-uri", "https://starburst.io")
                .put("openapi.cast-policy", castPolicy.name())
                .buildOrThrow();
        return new OpenApiConnectorFactory().create("openapi", config, new TestingConnectorContext());
    }

    private Throwable getConfigurationThrowable(String location)
    {
        return getConfigurationThrowable(location, CastPolicy.ERROR);
    }

    private Throwable getConfigurationThrowable(String location, CastPolicy castPolicy)
    {
        Collection<Message> creationMessages = assertThatThrownBy(() -> createConnector(location, castPolicy))
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

    @Test
    public void testUnusedSecuritySchemeProperties()
    {
        String petstore = requireNonNull(getClass().getClassLoader().getResource("petstore.yaml")).getFile();
        Set<String> messages = assertThat(getAppConfigException(ImmutableMap.<String, String>builder()
                .put("openapi.description-location", petstore)
                .put("openapi.base-uri", "https://starburst.io")
                .put("openapi.security-scheme.secret", "MY_SECRET")
                .put("openapi.security-scheme.in", "HEADER")
                .put("openapi.security-scheme.name", "X-Api-Key")
                .put("openapi.security-scheme.token-url", "https://example.org")
                .put("openapi.security-scheme.client-secret", "secret")
                .put("openapi.security-scheme.client-id", "client")
                .put("openapi.security-scheme.scopes", "read,write")
                .buildOrThrow()))
                .extracting(ApplicationConfigurationException::getErrors)
                .asInstanceOf(set(Message.class))
                .actual()
                .stream()
                .map(Message::getMessage)
                .collect(toImmutableSet());

        Set<String> messagePrefixes = ImmutableSet.<String>builder()
                .add("Configuration property 'openapi.security-scheme.client-id' was not used.")
                .add("Configuration property 'openapi.security-scheme.client-secret' was not used.")
                .add("Configuration property 'openapi.security-scheme.in' was not used.")
                .add("Configuration property 'openapi.security-scheme.name' was not used.")
                .add("Configuration property 'openapi.security-scheme.scopes' was not used.")
                .add("Configuration property 'openapi.security-scheme.secret' was not used.")
                .add("Configuration property 'openapi.security-scheme.token-url' was not used.")
                .build();
        assertThat(messages).hasSize(messagePrefixes.size());
        assertThat(messagePrefixes).allSatisfy(prefix ->
                assertThat(messages).anyMatch(message -> message.startsWith(prefix)));
    }

    private static ApplicationConfigurationException getAppConfigException(Map<String, String> config)
    {
        return assertThatThrownBy(() -> new OpenApiConnectorFactory()
                .create("openapi", config, new TestingConnectorContext())
                .shutdown())
                .asInstanceOf(throwable(ApplicationConfigurationException.class))
                .actual();
    }
}
