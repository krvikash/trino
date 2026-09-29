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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.starburst.materialization.ir.IrFixtures.builtinSignature;
import static io.starburst.materialization.ir.IrFixtures.outputOverScan;
import static io.starburst.materialization.ir.IrFixtures.reference;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static org.assertj.core.api.Assertions.assertThat;

class TestIrVersions
{
    private static final List<String> PREFIXES = ImmutableList.of(
            IrVersions.EXPRESSION_PREFIX,
            IrVersions.FUNCTION_PREFIX,
            IrVersions.CAST_PREFIX,
            IrVersions.LEAF_PREFIX);

    @Test
    void testOperationTreeStampsBareNames()
    {
        assertThat(IrVersions.collect(outputOverScan(new Symbol(BIGINT, "a"))))
                .isEqualTo(ImmutableMap.of("Output", 1, "TableScan", 1));
    }

    @Test
    void testOperationVersionsAreBareNames()
    {
        assertThat(IrVersions.OPERATION_VERSIONS).isEqualTo(ImmutableMap.of("Output", 1, "TableScan", 1));
        assertThat(IrVersions.SUPPORTED).containsAllEntriesOf(IrVersions.OPERATION_VERSIONS);
    }

    @Test
    void testNamespacesAreDisjoint()
    {
        for (String prefix : PREFIXES) {
            assertThat(prefix).endsWith(":");
            assertThat(PREFIXES).filteredOn(other -> !other.equals(prefix) && other.startsWith(prefix)).isEmpty();
        }
        assertThat(IrVersions.OPERATION_VERSIONS.keySet()).allSatisfy(name -> assertThat(name).doesNotContain(":"));
        assertEveryNameHasPrefix(IrVersions.EXPRESSION_VERSIONS, IrVersions.EXPRESSION_PREFIX);
        assertEveryNameHasPrefix(IrVersions.FUNCTION_VERSIONS, IrVersions.FUNCTION_PREFIX);
        assertEveryNameHasPrefix(IrVersions.CAST_VERSIONS, IrVersions.CAST_PREFIX);
        assertEveryNameHasPrefix(IrVersions.LEAF_VERSIONS, IrVersions.LEAF_PREFIX);
        assertThat(IrVersions.SUPPORTED).hasSize(
                IrVersions.OPERATION_VERSIONS.size()
                        + IrVersions.EXPRESSION_VERSIONS.size()
                        + IrVersions.FUNCTION_VERSIONS.size()
                        + IrVersions.CAST_VERSIONS.size()
                        + IrVersions.LEAF_VERSIONS.size());
    }

    @Test
    void testReferenceStampsNodeAndSymbolLeaf()
    {
        assertThat(IrVersions.collect(new Reference(new Symbol(BIGINT, "a"))))
                .isEqualTo(ImmutableMap.of("expr:Reference", 1, "leaf:Symbol", 1));
    }

    @Test
    void testCastStampsPairKey()
    {
        assertThat(IrVersions.CAST_VERSIONS).containsEntry("cast:varchar->varchar", 1);
        assertThat(IrVersions.collect(new Cast(new Reference(new Symbol(VARCHAR, "a")), createVarcharType(10))))
                .isEqualTo(ImmutableMap.of(
                        "expr:Cast", 1,
                        "leaf:Type", 1,
                        "cast:varchar->varchar", 1,
                        "expr:Reference", 1,
                        "leaf:Symbol", 1));
    }

    @Test
    void testFunctionKeyIsLowercaseQualifiedName()
    {
        assertThat(IrVersions.FUNCTION_VERSIONS).containsEntry("fn:system.builtin.$operator$equal", 1);
        assertThat(IrVersions.FUNCTION_VERSIONS.keySet()).allSatisfy(name -> assertThat(name).isLowerCase());
    }

    @Test
    void testNestedCallsStampEveryFunction()
    {
        Call equal = new Call(builtinSignature("$operator$EQUAL", BOOLEAN, BIGINT, BIGINT), ImmutableList.of(reference(BIGINT, "a"), reference(BIGINT, "b")));
        Call notEqual = new Call(builtinSignature("$not", BOOLEAN, BOOLEAN), ImmutableList.of(equal));
        assertThat(IrVersions.collect(notEqual)).isEqualTo(ImmutableMap.of(
                "expr:Call", 1,
                "leaf:BoundSignature", 1,
                "fn:system.builtin.$not", 1,
                "fn:system.builtin.$operator$equal", 1,
                "expr:Reference", 1,
                "leaf:Symbol", 1));
    }

    @Test
    void testRowFieldAccessStampsFieldReference()
    {
        assertThat(IrVersions.collect(new FieldReference(reference(IrJson.PERSON_ROW, "info"), 0)))
                .isEqualTo(ImmutableMap.of("expr:FieldReference", 1, "expr:Reference", 1, "leaf:Symbol", 1));
    }

    @Test
    void testTreeWithEveryExpressionStampsGoldenNameSet()
    {
        Expression tree = new Logical(Logical.Operator.AND, ImmutableList.of(
                new Call(
                        builtinSignature("$operator$EQUAL", BOOLEAN, VARCHAR, VARCHAR),
                        ImmutableList.of(
                                new FieldReference(reference(IrJson.PERSON_ROW, "info"), 0),
                                new Cast(reference(createVarcharType(10), "a"), VARCHAR))),
                new In(reference(BIGINT, "b"), ImmutableList.of(reference(BIGINT, "c"))),
                new IsNull(reference(BIGINT, "d"))));

        Set<Class<?>> nodeClasses = new HashSet<>();
        collectNodeClasses(tree, nodeClasses);
        assertThat(nodeClasses).containsExactlyInAnyOrder(Expression.class.getPermittedSubclasses());

        Map<String, Integer> versions = IrVersions.collect(tree);
        assertThat(versions).isEqualTo(ImmutableMap.builder()
                .put("expr:Call", 1)
                .put("expr:Cast", 1)
                .put("expr:FieldReference", 1)
                .put("expr:In", 1)
                .put("expr:IsNull", 1)
                .put("expr:Logical", 1)
                .put("expr:Reference", 1)
                .put("fn:system.builtin.$operator$equal", 1)
                .put("cast:varchar->varchar", 1)
                .put("leaf:BoundSignature", 1)
                .put("leaf:Symbol", 1)
                .put("leaf:Type", 1)
                .buildOrThrow());
        assertThat(IrVersions.SUPPORTED).containsAllEntriesOf(versions);
    }

    private static void collectNodeClasses(IrNode node, Set<Class<?>> nodeClasses)
    {
        nodeClasses.add(node.getClass());
        node.children().forEach(child -> collectNodeClasses(child, nodeClasses));
    }

    private static void assertEveryNameHasPrefix(Map<String, Integer> versions, String prefix)
    {
        assertThat(versions).isNotEmpty();
        assertThat(versions.keySet()).allSatisfy(name -> assertThat(name).startsWith(prefix));
    }
}
