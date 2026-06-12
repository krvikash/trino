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
package io.trino.sql.planner.exploratory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.sql.dialect.trino.operation.Constant;
import io.trino.sql.dialect.trino.operation.Lambda;
import io.trino.sql.dialect.trino.operation.Logical;
import io.trino.sql.dialect.trino.operation.Return;
import io.trino.sql.dialect.trino.operation.Row;
import io.trino.sql.dialect.trino.operation.Values;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Operation.AttributeKey;
import io.trino.sql.newir.Program;
import io.trino.sql.planner.exploratory.ReuseUtils.BlockAndValue;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Optional;
import java.util.Set;

import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.RowType.anonymousRow;
import static io.trino.sql.dialect.ir.IrDialect.HAS_SIDE_EFFECTS;
import static io.trino.sql.dialect.ir.IrDialect.IR;
import static io.trino.sql.dialect.ir.IrDialect.REPEATABILITY;
import static io.trino.sql.dialect.ir.IrDialect.Repeatability.DETERMINISTIC;
import static io.trino.sql.dialect.ir.IrDialect.SAFE;
import static io.trino.sql.dialect.memo.MemoDialect.MEMO;
import static io.trino.sql.dialect.memo.MemoDialect.REUSE_ID;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static io.trino.sql.dialect.trino.operationmetadata.LogicalOperationMetadata.LogicalOperator.AND;
import static io.trino.sql.newir.DialectRegistry.TESTING_DIALECT_REGISTRY;
import static io.trino.sql.planner.exploratory.MemoGroupMatcher.memoGroup;
import static io.trino.sql.planner.exploratory.MemoOperationMatcher.GroupChildMatcher.groupChild;
import static io.trino.sql.planner.exploratory.MemoOperationMatcher.memoOperation;
import static io.trino.sql.planner.exploratory.MemoTestingHelper.program;
import static io.trino.sql.planner.exploratory.MemoTestingHelper.valuesOfRows;
import static io.trino.sql.planner.exploratory.MemoTestingUtil.assertMemoGroupsContains;
import static io.trino.sql.planner.exploratory.MemoTestingUtil.attributes;
import static io.trino.sql.planner.exploratory.ReuseUtils.getReusedOperations;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class TestReuseInMemo
{
    /**
     * No reused operations.
     * <pre>
     * ^block
     * { constantNull
     *   outerLambda
     *      { constantFalse
     *        innerLambda
     *          { constantTrue
     *            and(constantTrue, constantFalse, constantNull)}}}
     * </pre>
     */
    @Test
    public void testNoReuse()
    {
        Constant constantTrue = new Constant("%constant_true", BOOLEAN, true);
        Constant constantFalse = new Constant("%constant_false", BOOLEAN, false);
        Constant constantNull = new Constant("%constant_null", BOOLEAN, null);
        Logical andTrueFalseNull = and("%and_true_false_null", constantTrue, constantFalse, constantNull);
        Lambda innerLambda = lambda("%inner_lambda", new Block.Parameter("%inner_param", irType(anonymousRow(BOOLEAN))), constantTrue, andTrueFalseNull);
        Lambda outerLambda = lambda("%outer_lambda", new Block.Parameter("%outer_param", irType(anonymousRow(BOOLEAN))), constantFalse, innerLambda);
        Block block = block(constantNull, outerLambda);

        assertThat(getReusedOperations(block)).isEqualTo(Set.of());
    }

    /**
     * Identical operations in both lambdas: constantNull.
     * They are not reused but declared in different scopes.
     * <pre>
     * ^block
     * { constantTrue
     *   constantFalse
     *   lambda1
     *      { constantNull
     *        and(constantTrue, constantNull)}
     *   lambda2
     *      { constantNull
     *        and(constantFalse, constantNull)}}
     * </pre>
     */
    @Test
    public void testSameResultNoReuse()
    {
        Constant constantTrue = new Constant("%constant_true", BOOLEAN, true);
        Constant constantFalse = new Constant("%constant_false", BOOLEAN, false);
        Constant constantNull = new Constant("%constant_null", BOOLEAN, null);
        Logical andTrueNull = and("%and_true_null", constantTrue, constantNull);
        Logical andFalseNull = and("%and_false_null", constantFalse, constantNull);
        Lambda lambda1 = lambda("%lambda_1", new Block.Parameter("%param_1", irType(anonymousRow(BOOLEAN))), constantNull, andTrueNull);
        Lambda lambda2 = lambda("%lambda_2", new Block.Parameter("%param_2", irType(anonymousRow(BOOLEAN))), constantNull, andFalseNull);
        Block block = block(constantTrue, constantFalse, lambda1, lambda2);

        assertThat(getReusedOperations(block)).isEqualTo(Set.of());
    }

    /**
     * Both logical operations reuse the same constantTrue operation.
     * Block id for the reused operation declaration is 0 (top level).
     * <pre>
     * ^block
     * { constantTrue
     *   constantFalse
     *   constantNull
     *   and(constantTrue, constantNull)}
     *   and(constantFalse, constantTrue)}}
     * </pre>
     */
    @Test
    public void testTopLevelReuse()
    {
        Constant constantTrue = new Constant("%constant_true", BOOLEAN, true);
        Constant constantFalse = new Constant("%constant_false", BOOLEAN, false);
        Constant constantNull = new Constant("%constant_null", BOOLEAN, null);
        Logical andTrueNull = and("%and_true_null", constantTrue, constantNull);
        Logical andFalseTrue = and("%and_false_true", constantFalse, constantTrue);
        Block block = block(constantTrue, constantFalse, constantNull, andTrueNull, andFalseTrue);

        assertThat(getReusedOperations(block)).isEqualTo(Set.of(new BlockAndValue(0, constantTrue.result())));
    }

    /**
     * In the second lambda, constantNull is reused.
     * Block id for the reused operation declaration is 2 (third block in traversal order).
     * <pre>
     * ^block
     * { constantTrue
     *   lambda1
     *      { constantFalse
     *        and(constantTrue, constantFalse)}
     *   lambda2
     *      { constantNull
     *        and(constantNull, constantNull)}}
     * </pre>
     */
    @Test
    public void testNestedLevelReuse()
    {
        Constant constantTrue = new Constant("%constant_true", BOOLEAN, true);
        Constant constantFalse = new Constant("%constant_false", BOOLEAN, false);
        Constant constantNull = new Constant("%constant_null", BOOLEAN, null);
        Logical andTrueFalse = and("%and_true_false", constantTrue, constantFalse);
        Logical andNullNull = and("%and_null_null", constantNull, constantNull);
        Lambda lambda1 = lambda("%lambda_1", new Block.Parameter("%param_1", irType(anonymousRow(BOOLEAN))), constantFalse, andTrueFalse);
        Lambda lambda2 = lambda("%lambda_2", new Block.Parameter("%param_2", irType(anonymousRow(BOOLEAN))), constantNull, andNullNull);
        Block block = block(constantTrue, lambda1, lambda2);

        assertThat(getReusedOperations(block)).isEqualTo(Set.of(new BlockAndValue(2, constantNull.result())));
    }

    /**
     * constantTrue is reused: in lambda and on top level.
     * Block id for the reused operation declaration is 0 (top level).
     * <pre>
     * ^block
     * { constantTrue
     *   constantFalse
     *   lambda
     *      { constantNull
     *        and(constantTrue, constantNull)}
     *   and(constantTrue, constantFalse)}
     * </pre>
     */
    @Test
    public void testMixedLevelReuse()
    {
        Constant constantTrue = new Constant("%constant_true", BOOLEAN, true);
        Constant constantFalse = new Constant("%constant_false", BOOLEAN, false);
        Constant constantNull = new Constant("%constant_null", BOOLEAN, null);
        Logical andTrueNull = and("%and_true_null", constantTrue, constantNull);
        Logical andTrueFalse = and("%and_true_false", constantTrue, constantFalse);
        Lambda lambda = lambda("%lambda", new Block.Parameter("%param", irType(anonymousRow(BOOLEAN))), constantNull, andTrueNull);
        Block block = block(constantTrue, constantFalse, lambda, andTrueFalse);

        assertThat(getReusedOperations(block)).isEqualTo(Set.of(new BlockAndValue(0, constantTrue.result())));
    }

    /**
     * constantTrue is reused: in lambda and on top level. Block id for the reused constantTrue operation declaration is 0 (top level).
     * constantNull is reused: two references in lambda. Block id for the reused constantNull operation declaration is 1 (second block in traversal order).
     * <pre>
     * ^block
     * { constantTrue
     *   constantFalse
     *   lambda
     *      { constantNull
     *        and(constantTrue, constantNull, constantNull)}
     *   and(constantTrue, constantFalse)}
     * </pre>
     */
    @Test
    public void testMultipleReuses()
    {
        Constant constantTrue = new Constant("%constant_true", BOOLEAN, true);
        Constant constantFalse = new Constant("%constant_false", BOOLEAN, false);
        Constant constantNull = new Constant("%constant_null", BOOLEAN, null);
        Logical andTrueNullNull = and("%and_true_null_null", constantTrue, constantNull, constantNull);
        Logical andTrueFalse = and("%and_true_false", constantTrue, constantFalse);
        Lambda lambda = lambda("%lambda", new Block.Parameter("%param", irType(anonymousRow(BOOLEAN))), constantNull, andTrueNullNull);
        Block block = block(constantTrue, constantFalse, lambda, andTrueFalse);

        assertThat(getReusedOperations(block)).isEqualTo(Set.of(
                new BlockAndValue(0, constantTrue.result()),
                new BlockAndValue(1, constantNull.result())));
    }

    /**
     * The block contains three declaration instances of identical operation (constant true).
     * Some of the instances are deduplicated in Memo, but the reuse information is preserved.
     * - constantTrue1 is reused on top level and in lambda.
     * - constantTrue2 is reused in lambda.
     * - constantTrue3 is not reused (used once).
     * <pre>
     * ^block
     * { constantTrue1
     *   lambda
     *      { constantTrue2
     *        and(constantTrue1, constantTrue2, constantTrue2)}
     *   constantTrue3
     *   and(constantTrue1, constantTrue3)}
     * </pre>
     */
    @Test
    public void testMultipleReuseSameOperation()
    {
        Constant constantTrue1 = new Constant("%constant_true_1", BOOLEAN, true);
        Constant constantTrue2 = new Constant("%constant_true_2", BOOLEAN, true);
        Constant constantTrue3 = new Constant("%constant_true_3", BOOLEAN, true);
        Logical andTrue1True2True2 = and("%and_true1_true2_true2", constantTrue1, constantTrue2, constantTrue2);
        Logical andTrue1True3 = and("%and_true1_true3", constantTrue1, constantTrue3);
        Lambda lambda = lambda("%lambda", new Block.Parameter("%param", irType(anonymousRow(BOOLEAN))), constantTrue2, andTrue1True2True2);
        Block block = block(constantTrue1, lambda, constantTrue3, andTrue1True3);

        assertThat(getReusedOperations(block)).isEqualTo(Set.of(
                new BlockAndValue(0, constantTrue1.result()),
                new BlockAndValue(1, constantTrue2.result())));
    }

    /**
     * Missing declaration of constantFalse (not included in the block).
     * <pre>
     * ^block
     * { constantTrue
     *   and(constantTrue, constantFalse)}
     * </pre>
     */
    @Test
    public void testInvalidBlockMissingDeclaration()
    {
        Constant constantTrue = new Constant("%constant_true", BOOLEAN, true);
        Constant constantFalse = new Constant("%constant_false", BOOLEAN, false);
        Logical andTrueFalse = and("%and_true_false", constantTrue, constantFalse);
        Block block = block(constantTrue, andTrueFalse);

        assertThatThrownBy(() -> getReusedOperations(block))
                .hasMessage("invalid program: use of undeclared operation result: %constant_false");
    }

    /**
     * Ambiguous declaration of constantTrue: one on top level and one in lambda. Both are visible in lambda.
     * <pre>
     * ^block
     * { constantTrue
     *   lambda
     *      { constantFalse
     *        constantTrue
     *        and(constantTrue, constantFalse)}}
     * </pre>
     */
    @Test
    public void testInvalidBlockAmbiguousDeclaration()
    {
        Constant constantTrue = new Constant("%constant_true", BOOLEAN, true);
        Constant constantFalse = new Constant("%constant_false", BOOLEAN, false);
        Logical andTrueFalse = and("%and_true_false", constantTrue, constantFalse);
        Lambda lambda = lambda("%lambda", new Block.Parameter("%param", irType(anonymousRow(BOOLEAN))), constantFalse, constantTrue, andTrueFalse);
        Block block = block(constantTrue, lambda);

        assertThatThrownBy(() -> getReusedOperations(block))
                .hasMessage("invalid program: duplicate declaration: %constant_true");
    }

    /**
     * The block contains three declaration instances of identical operation (constant true).
     * All instances are deduplicated in Memo, but the reuse information is preserved.
     * - constantTrue1 is used three times.
     * - constantTrue2 is used two times.
     * - constantTrue3 is not reused (used once).
     * <pre>
     * ^block
     * { constantTrue1
     *   constantTrue2
     *   constantTrue3
     *   logical(constantTrue1, constantTrue1)
     *   logical(constantTrue1, constantTrue2)
     *   logical(constantTrue2, constantTrue3)}
     * </pre>
     * The created Block is wrapped in Row, Values, Output, and Query to form a valid Trino Program.
     */
    @Test
    public void testSimpleReuseInMemo()
    {
        Constant constantTrue1 = new Constant("%constant_true_1", BOOLEAN, true);
        Constant constantTrue2 = new Constant("%constant_true_2", BOOLEAN, true);
        Constant constantTrue3 = new Constant("%constant_true_3", BOOLEAN, true);
        Logical logicalTrue1True1 = and("%logical_true1_true1", constantTrue1, constantTrue1);
        Logical logicalTrue1True2 = and("%logical_true1_true2", constantTrue1, constantTrue2);
        Logical logicalTrue2True3 = and("%logical_true2_true3", constantTrue2, constantTrue3);
        Row row = new Row("%row", ImmutableList.of(logicalTrue2True3.result()), ImmutableList.of(logicalTrue2True3.attributes()));
        Block block = block(constantTrue1, constantTrue2, constantTrue3, logicalTrue1True1, logicalTrue1True2, logicalTrue2True3, row);

        Values values = valuesOfRows(ImmutableList.of(block.operations()));
        Program program = program(values);
        Memo memo = Memo.forProgram(program, TESTING_DIALECT_REGISTRY);

        assertMemoGroupsContains(
                memo.groups(),
                ImmutableMap.of(
                        // deduplicated constant operations
                        0, memoGroup()
                                .withOperations(memoOperation()
                                        .withName("constant")
                                        .withResultType(irType(BOOLEAN))
                                        .build())
                                .build(),
                        // reuse of group 0 for constantTrue1, reuseId = 0
                        1, memoGroup()
                                .withOperations(memoOperation()
                                        .withName("reuse")
                                        .withResultType(irType(BOOLEAN))
                                        .withChildren(groupChild(0))
                                        .withAttributes(attributes(
                                                new AttributeKey(IR, REPEATABILITY),
                                                DETERMINISTIC,
                                                new AttributeKey(IR, SAFE),
                                                true,
                                                new AttributeKey(IR, HAS_SIDE_EFFECTS),
                                                false,
                                                new AttributeKey(MEMO, REUSE_ID),
                                                0))
                                        .build())
                                .build(),
                        // reuse of group 0 for constantTrue2, reuseId = 1
                        2, memoGroup()
                                .withOperations(memoOperation()
                                        .withName("reuse")
                                        .withResultType(irType(BOOLEAN))
                                        .withChildren(groupChild(0))
                                        .withAttributes(attributes(
                                                new AttributeKey(IR, REPEATABILITY),
                                                DETERMINISTIC,
                                                new AttributeKey(IR, SAFE),
                                                true,
                                                new AttributeKey(IR, HAS_SIDE_EFFECTS),
                                                false,
                                                new AttributeKey(MEMO, REUSE_ID),
                                                1))
                                        .build())
                                .build(),
                        // logical(constantTrue1, constantTrue1) -- references reused constantTrue1 (group 1)
                        3, memoGroup()
                                .withOperations(memoOperation()
                                        .withName("logical")
                                        .withChildren(groupChild(1), groupChild(1))
                                        .build())
                                .build(),
                        // logical(constantTrue1, constantTrue2) -- references reused constantTrue1 (group 1) and reused constantTrue2 (group 2)
                        4, memoGroup()
                                .withOperations(memoOperation()
                                        .withName("logical")
                                        .withChildren(groupChild(1), groupChild(2))
                                        .build())
                                .build(),
                        // logical(constantTrue2, constantTrue3) -- references reused constantTrue2 (group 2) and non-reused constantTrue3 (directly group 0)
                        5, memoGroup()
                                .withOperations(memoOperation()
                                        .withName("logical")
                                        .withChildren(groupChild(2), groupChild(0))
                                        .build())
                                .build()));
    }

    /**
     * Same structure as in testMultipleReuseSameOperation.
     * The block contains three declaration instances of identical operation (constant true).
     * Both instances declared on top level (constantTrue1 and constantTrue3) are deduplicated in Memo, and the reuse information is preserved.
     * The third instance constantTrue2 in lambda is not deduplicated due to different parameters.
     * - constantTrue1 is reused on top level and in lambda.
     * - constantTrue2 is reused in lambda.
     * - constantTrue3 is not reused (used once).
     * <pre>
     * ^block
     * { constantTrue1
     *   lambda
     *      { constantTrue2
     *        and(constantTrue1, constantTrue2, constantTrue2)}
     *   constantTrue3
     *   and(constantTrue1, constantTrue3)}
     * </pre>
     * The created Block is wrapped in Row, Values, Output, and Query to form a valid Trino Program.
     */
    @Test
    public void testReuseInMemo()
    {
        Constant constantTrue1 = new Constant("%constant_true_1", BOOLEAN, true);
        Constant constantTrue2 = new Constant("%constant_true_2", BOOLEAN, true);
        Constant constantTrue3 = new Constant("%constant_true_3", BOOLEAN, true);
        Logical andTrue1True2True2 = and("%and_true1_true2_true2", constantTrue1, constantTrue2, constantTrue2);
        Logical andTrue1True3 = and("%and_true1_true3", constantTrue1, constantTrue3);
        Lambda lambda = lambda("%lambda", new Block.Parameter("%param", irType(anonymousRow(BOOLEAN))), constantTrue2, andTrue1True2True2);
        Row row = new Row("%row", ImmutableList.of(andTrue1True3.result()), ImmutableList.of(andTrue1True3.attributes()));
        Block block = block(constantTrue1, lambda, constantTrue3, andTrue1True3, row);

        Values values = valuesOfRows(ImmutableList.of(block.operations()));
        Program program = program(values);
        Memo memo = Memo.forProgram(program, TESTING_DIALECT_REGISTRY);

        assertMemoGroupsContains(
                memo.groups(),
                ImmutableMap.of(
                        // deduplicated constantTrue1 and constantTrue3 operations
                        0, memoGroup()
                                .withGroupParameterTypes()
                                .withOperations(memoOperation()
                                        .withName("constant")
                                        .withResultType(irType(BOOLEAN))
                                        .build())
                                .build(),
                        // reuse of group 0 for constantTrue1, reuseId = 0
                        1, memoGroup()
                                .withGroupParameterTypes()
                                .withOperations(memoOperation()
                                        .withName("reuse")
                                        .withResultType(irType(BOOLEAN))
                                        .withChildren(groupChild(0))
                                        .withAttributes(attributes(
                                                new AttributeKey(IR, REPEATABILITY),
                                                DETERMINISTIC,
                                                new AttributeKey(IR, SAFE),
                                                true,
                                                new AttributeKey(IR, HAS_SIDE_EFFECTS),
                                                false,
                                                new AttributeKey(MEMO, REUSE_ID),
                                                0))
                                        .build())
                                .build(),
                        // constantTrue2 operation in lambda is not deduplicated
                        2, memoGroup()
                                .withGroupParameterTypes(irType(anonymousRow(BOOLEAN)))
                                .withOperations(memoOperation()
                                        .withName("constant")
                                        .withResultType(irType(BOOLEAN))
                                        .build())
                                .build(),
                        // reuse of group 2 for constantTrue2, reuseId = 1
                        3, memoGroup()
                                .withGroupParameterTypes(irType(anonymousRow(BOOLEAN)))
                                .withOperations(memoOperation()
                                        .withName("reuse")
                                        .withResultType(irType(BOOLEAN))
                                        .withChildren(groupChild(2))
                                        .withAttributes(attributes(
                                                new AttributeKey(IR, REPEATABILITY),
                                                DETERMINISTIC,
                                                new AttributeKey(IR, SAFE),
                                                true,
                                                new AttributeKey(IR, HAS_SIDE_EFFECTS),
                                                false,
                                                new AttributeKey(MEMO, REUSE_ID),
                                                1))
                                        .build())
                                .build(),
                        // and(constantTrue1, constantTrue2, constantTrue2) in lambda -- references reused constantTrue1 (group 1) and reused constantTrue2 (group 3)
                        4, memoGroup()
                                .withGroupParameterTypes(irType(anonymousRow(BOOLEAN)))
                                .withOperations(memoOperation()
                                        .withName("logical")
                                        .withChildren(groupChild(1), groupChild(3), groupChild(3))
                                        .build())
                                .build(),
                        // and(constantTrue1, constantTrue3) on top level -- references reused constantTrue1 (group 1) and non-reused constantTrue3 (directly group 0)
                        7, memoGroup()
                                .withGroupParameterTypes()
                                .withOperations(memoOperation()
                                        .withName("logical")
                                        .withResultType(irType(BOOLEAN))
                                        .withChildren(groupChild(1), groupChild(0))
                                        .build())
                                .build()));
    }

    private static Logical and(String name, Constant... terms)
    {
        return new Logical(name, Arrays.stream(terms).map(Constant::result).collect(toList()), AND, Arrays.stream(terms).map(Constant::attributes).collect(toList()));
    }

    private static Lambda lambda(String name, Block.Parameter parameter, Operation... operations)
    {
        Block.Builder lambdaBody = new Block.Builder(Optional.of("^lambda"), ImmutableList.of(parameter));
        Arrays.stream(operations).forEach(lambdaBody::addOperation);
        Operation recentOperation = lambdaBody.recentOperation();
        lambdaBody.addOperation(new Return(name + "_return", recentOperation.result(), recentOperation.attributes()));
        return new Lambda(name, lambdaBody.build());
    }

    private static Block block(Operation... operations)
    {
        Block.Builder builder = new Block.Builder(Optional.of("^block"), ImmutableList.of());
        Arrays.stream(operations).forEach(builder::addOperation);
        Operation recentOperation = builder.recentOperation();
        builder.addOperation(new Return("%block_return", recentOperation.result(), recentOperation.attributes()));
        return builder.build();
    }
}
