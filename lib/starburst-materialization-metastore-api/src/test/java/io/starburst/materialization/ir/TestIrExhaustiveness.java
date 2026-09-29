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
package io.starburst.materialization.ir;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.provider.Arguments;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Every IR node type must be registered in {@link IrVersions#SUPPORTED}, in its interface's {@link JsonSubTypes}
 * and in the {@link TestIrWireFormat} fixtures, so a node cannot be added without a version and a pinned wire format.
 */
class TestIrExhaustiveness
{
    @Test
    void testExpressionsAreRegistered()
    {
        assertRegistered(Expression.class, IrVersions.EXPRESSION_PREFIX, TestIrWireFormat.expressions());
    }

    @Test
    void testOperationsAreRegistered()
    {
        assertRegistered(Operation.class, "", TestIrWireFormat.operations());
    }

    private static void assertRegistered(Class<?> nodeInterface, String prefix, Stream<Arguments> wireFormatCases)
    {
        Map<String, Class<?>> jsonSubTypes = Arrays.stream(nodeInterface.getAnnotation(JsonSubTypes.class).value())
                .collect(toImmutableMap(JsonSubTypes.Type::name, JsonSubTypes.Type::value));
        Set<Class<?>> fixtureClasses = wireFormatCases
                .map(arguments -> arguments.get()[1].getClass())
                .collect(toImmutableSet());

        Class<?>[] permitted = nodeInterface.getPermittedSubclasses();
        assertThat(jsonSubTypes).hasSize(permitted.length);
        for (Class<?> nodeClass : permitted) {
            String name = staticField(nodeClass, "NAME", String.class);
            int version = staticField(nodeClass, "VERSION", Integer.class);
            assertThat(IrVersions.SUPPORTED).as("%s version", nodeClass.getSimpleName()).containsEntry(prefix + name, version);
            assertThat(jsonSubTypes).as("%s JSON subtype", nodeClass.getSimpleName()).containsEntry(name, nodeClass);
            assertThat(fixtureClasses).as("%s wire format case", nodeClass.getSimpleName()).contains(nodeClass);
            assertThat(TestIrWireFormat.fixtureExists(name)).as("%s fixture", nodeClass.getSimpleName()).isTrue();
        }
    }

    private static <T> T staticField(Class<?> nodeClass, String field, Class<T> type)
    {
        try {
            return type.cast(nodeClass.getDeclaredField(field).get(null));
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError("%s has no static %s".formatted(nodeClass.getSimpleName(), field), e);
        }
    }
}
