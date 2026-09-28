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
package io.trino.sql.planner.optimizations.ctereuse;

import com.google.common.collect.ImmutableList;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.dialect.trino.operation.Array;
import io.trino.sql.dialect.trino.operation.Constant;
import io.trino.sql.dialect.trino.operation.FieldReference;
import io.trino.sql.dialect.trino.operation.Filter;
import io.trino.sql.dialect.trino.operation.In;
import io.trino.sql.dialect.trino.operation.IsNull;
import io.trino.sql.dialect.trino.operation.Lambda;
import io.trino.sql.dialect.trino.operation.Logical;
import io.trino.sql.dialect.trino.operation.Output;
import io.trino.sql.dialect.trino.operation.Query;
import io.trino.sql.dialect.trino.operation.Return;
import io.trino.sql.dialect.trino.operation.Row;
import io.trino.sql.dialect.trino.operation.Values;
import io.trino.sql.dialect.trino.operationmetadata.LogicalOperationMetadata.LogicalOperator;
import io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.ConstantValue;
import io.trino.sql.newir.Attributes;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Program;
import io.trino.sql.newir.Value;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.RowType.anonymousRow;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.sql.dialect.ir.IrDialect.DEFAULT_BLOCK_PARAMETER_ATTRIBUTES;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static io.trino.sql.dialect.trino.operationmetadata.ConstantOperationMetadata.CONSTANT_VALUE;
import static io.trino.sql.planner.optimizations.ctereuse.InListConstantFolding.foldConstantInLists;
import static io.trino.sql.planner.optimizations.ctereuse.StructuralEquivalenceUtils.blocksStructurallyEquivalent;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestInListConstantFolding
{
    @Test
    public void testFoldConstantInListRecursively()
    {
        Lambda lambda = new Lambda("%lambda", inPredicate("nested", BIGINT, ImmutableList.of(2L, 3L)));
        Block predicate = predicateContaining(lambda);
        ProgramAndSource programAndSource = programWithPredicate(predicate);

        Program folded = foldConstantInLists(programAndSource.program());

        Query query = (Query) folded.root();
        assertThat(query.query().operations().getFirst()).isSameAs(programAndSource.source());
        Filter filter = (Filter) query.query().operations().get(1);
        Lambda foldedLambda = (Lambda) filter.predicate().operations().getFirst();
        assertThat(foldedLambda.lambdaBody().operations())
                .extracting(Object::getClass)
                .containsExactly(FieldReference.class, Constant.class, In.class, Return.class);
    }

    @Test
    public void testFoldConstantInListsRepeatedly()
    {
        Lambda lambda = new Lambda("%lambda", inPredicate("nested", BIGINT, ImmutableList.of(2L, 3L)));
        Program program = programWithPredicate(predicateContaining(lambda)).program();

        Program folded = foldConstantInLists(program);

        assertThat(foldConstantInLists(folded)).isSameAs(folded);
    }

    @Test
    public void testRejectOuterOperationResultReferencedFromNestedBlock()
    {
        Block.Parameter predicateParameter = new Block.Parameter("%predicate_parameter", irType(anonymousRow(BIGINT)));
        Constant escaped = new Constant("%escaped", BIGINT, 2L);

        Block.Parameter lambdaParameter = new Block.Parameter("%lambda_parameter", irType(anonymousRow(BIGINT)));
        FieldReference input = new FieldReference("%input", lambdaParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Array valueList = new Array(
                "%value_list",
                BIGINT,
                ImmutableList.of(escaped.result()),
                ImmutableList.of(escaped.attributes()));
        In in = new In("%in", input.result(), valueList.result(), ImmutableList.of(input.attributes(), valueList.attributes()));
        Return lambdaReturn = new Return("%lambda_return", in.result(), in.attributes());
        Lambda lambda = new Lambda(
                "%lambda",
                new Block(
                        Optional.of("^lambda"),
                        ImmutableList.of(lambdaParameter),
                        ImmutableList.of(input, valueList, in, lambdaReturn)));

        Block predicate = booleanPredicate(
                predicateParameter,
                ImmutableList.of(escaped, lambda));

        assertThatThrownBy(() -> foldConstantInLists(programWithPredicate(predicate).program()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Operation result %escaped is referenced before its declaration or outside its declaring block");
    }

    @Test
    public void testRejectOperationResultReferencedBeforeDeclaration()
    {
        Block.Parameter predicateParameter = new Block.Parameter("%predicate_parameter", irType(anonymousRow(BIGINT)));
        Constant declaredLater = new Constant("%declared_later", BIGINT, 2L);
        IsNull forwardReference = new IsNull("%forward_reference", declaredLater.result(), declaredLater.attributes());
        Block predicate = booleanPredicate(
                predicateParameter,
                ImmutableList.of(forwardReference, declaredLater));

        assertThatThrownBy(() -> foldConstantInLists(programWithPredicate(predicate).program()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Operation result %declared_later is referenced before its declaration or outside its declaring block");
    }

    @Test
    public void testAllowOuterBlockParameterReferencedFromNestedBlock()
    {
        Block.Parameter predicateParameter = new Block.Parameter("%predicate_parameter", irType(anonymousRow(BIGINT)));
        Block.Parameter lambdaParameter = new Block.Parameter("%lambda_parameter", irType(anonymousRow(BIGINT)));
        FieldReference outerReference = new FieldReference("%outer_reference", predicateParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Return lambdaReturn = new Return("%lambda_return", outerReference.result(), outerReference.attributes());
        Lambda lambda = new Lambda(
                "%lambda",
                new Block(
                        Optional.of("^lambda"),
                        ImmutableList.of(lambdaParameter),
                        ImmutableList.of(outerReference, lambdaReturn)));
        ProgramAndSource programAndSource = programWithPredicate(booleanPredicate(predicateParameter, ImmutableList.of(lambda)));

        assertThat(foldConstantInLists(programAndSource.program()))
                .isSameAs(programAndSource.program());
    }

    @Test
    public void testFoldConstantInList()
    {
        Block block = inPredicate("test", BIGINT, asList(2L, null, 3L));
        Array array = (Array) block.operations().get(4);

        Block folded = foldPredicate(block);

        assertThat(folded.operations())
                .hasSize(4)
                .extracting(Object::getClass)
                .containsExactly(FieldReference.class, Constant.class, In.class, Return.class);
        Constant foldedArray = (Constant) folded.operations().get(1);
        assertThat(foldedArray.result()).isEqualTo(array.result());
        assertThat(CONSTANT_VALUE.getAttribute(foldedArray.attributes()))
                .isEqualTo(arrayConstant(BIGINT, asList(2L, null, 3L)));
    }

    @Test
    public void testDoesNotFoldNonConstantInList()
    {
        Block.Parameter parameter = new Block.Parameter("%parameter", irType(anonymousRow(BIGINT, BIGINT)));
        FieldReference input = new FieldReference("%input", parameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference listValue = new FieldReference("%list_value", parameter, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Constant constant = new Constant("%constant", BIGINT, 1L);
        Array valueList = new Array(
                "%value_list",
                BIGINT,
                ImmutableList.of(listValue.result(), constant.result()),
                ImmutableList.of(listValue.attributes(), constant.attributes()));
        In in = new In("%in", input.result(), valueList.result(), ImmutableList.of(input.attributes(), valueList.attributes()));
        Return returnOperation = new Return("%return", in.result(), in.attributes());
        Block block = new Block(
                Optional.empty(),
                ImmutableList.of(parameter),
                ImmutableList.of(input, listValue, constant, valueList, in, returnOperation));

        assertThat(foldPredicate(block)).isSameAs(block);
    }

    @Test
    public void testDoesNotFoldNonLocalConstant()
    {
        Constant input = new Constant("%input", BIGINT, 1L);
        Block.Parameter predicateParameter = new Block.Parameter("%predicate_parameter", irType(anonymousRow(BIGINT)));
        Block.Parameter outerConstant = new Block.Parameter("%outer_constant", irType(BIGINT));
        Array valueList = new Array(
                "%value_list",
                BIGINT,
                ImmutableList.of(outerConstant),
                ImmutableList.of(Attributes.empty()));
        In in = new In("%in", input.result(), valueList.result(), ImmutableList.of(input.attributes(), valueList.attributes()));
        Return returnOperation = new Return("%return", in.result(), in.attributes());
        Block block = new Block(
                Optional.empty(),
                ImmutableList.of(predicateParameter),
                ImmutableList.of(input, valueList, in, returnOperation));

        assertThat(foldPredicate(block)).isSameAs(block);
    }

    @Test
    public void testDoesNotFoldArrayUsedOutsideInValueList()
    {
        Block.Parameter predicateParameter = new Block.Parameter("%predicate_parameter", irType(anonymousRow(BIGINT)));
        Constant input = new Constant("%input", BIGINT, 1L);
        Constant listValue = new Constant("%list_value", BIGINT, 2L);
        Array valueList = new Array(
                "%value_list",
                BIGINT,
                ImmutableList.of(listValue.result()),
                ImmutableList.of(listValue.attributes()));
        In in = new In("%in", input.result(), valueList.result(), ImmutableList.of(input.attributes(), valueList.attributes()));
        IsNull isNull = new IsNull("%is_null", valueList.result(), valueList.attributes());
        Logical logical = new Logical(
                "%logical",
                ImmutableList.of(in.result(), isNull.result()),
                LogicalOperator.AND,
                ImmutableList.of(in.attributes(), isNull.attributes()));
        Return returnOperation = new Return("%return", logical.result(), logical.attributes());
        Block block = new Block(
                Optional.empty(),
                ImmutableList.of(predicateParameter),
                ImmutableList.of(input, listValue, valueList, in, isNull, logical, returnOperation));

        assertThat(foldPredicate(block)).isSameAs(block);
    }

    @Test
    public void testDoesNotFoldArrayWithUnsafeElementComparison()
    {
        Block block = inPredicate(
                "test",
                TIMESTAMP_TZ_MILLIS,
                ImmutableList.of(packDateTimeWithZone(1, UTC_KEY), packDateTimeWithZone(2, UTC_KEY)));

        assertThat(foldPredicate(block)).isSameAs(block);
    }

    @Test
    public void testRetainConstantUsedOutsideValueList()
    {
        Block.Parameter predicateParameter = new Block.Parameter("%predicate_parameter", irType(anonymousRow(BIGINT)));
        Constant input = new Constant("%input", BIGINT, 1L);
        Constant shared = new Constant("%shared", BIGINT, 2L);
        Constant other = new Constant("%other", BIGINT, 3L);
        Array valueList = new Array(
                "%value_list",
                BIGINT,
                ImmutableList.of(shared.result(), shared.result(), other.result()),
                ImmutableList.of(shared.attributes(), shared.attributes(), other.attributes()));
        In in = new In("%in", input.result(), valueList.result(), ImmutableList.of(input.attributes(), valueList.attributes()));
        IsNull isNull = new IsNull("%is_null", shared.result(), shared.attributes());
        Logical logical = new Logical(
                "%logical",
                ImmutableList.of(in.result(), isNull.result()),
                LogicalOperator.AND,
                ImmutableList.of(in.attributes(), isNull.attributes()));
        Return returnOperation = new Return("%return", logical.result(), logical.attributes());
        Block block = new Block(
                Optional.empty(),
                ImmutableList.of(predicateParameter),
                ImmutableList.of(input, shared, other, valueList, in, isNull, logical, returnOperation));

        Block folded = foldPredicate(block);

        assertThat(folded.operations()).contains(shared);
        assertThat(folded.operations()).doesNotContain(other);
        Constant foldedArray = folded.operations().stream()
                .filter(operation -> operation.result().equals(valueList.result()))
                .map(Constant.class::cast)
                .findFirst()
                .orElseThrow();
        assertThat(CONSTANT_VALUE.getAttribute(foldedArray.attributes()))
                .isEqualTo(arrayConstant(BIGINT, ImmutableList.of(2L, 2L, 3L)));
    }

    @Test
    public void testFoldedInStructuralEquivalence()
    {
        assertThat(blocksStructurallyEquivalent(
                foldPredicate(inPredicate("first", BIGINT, ImmutableList.of(2L, 3L))),
                foldPredicate(inPredicate("second", BIGINT, ImmutableList.of(2L, 3L)))))
                .isTrue();

        assertThat(blocksStructurallyEquivalent(
                foldPredicate(inPredicate("first", BIGINT, ImmutableList.of(2L, 3L))),
                foldPredicate(inPredicate("second", BIGINT, ImmutableList.of(2L, 4L)))))
                .isFalse();
    }

    private static Block inPredicate(String namePrefix, Type type, List<?> values)
    {
        Block.Parameter parameter = new Block.Parameter("%" + namePrefix + "_parameter", irType(anonymousRow(type)));
        FieldReference input = new FieldReference("%" + namePrefix + "_input", parameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        ImmutableList.Builder<Constant> constants = ImmutableList.builder();
        for (int index = 0; index < values.size(); index++) {
            constants.add(new Constant("%" + namePrefix + "_value_" + index, type, values.get(index)));
        }
        List<Constant> valueListElements = constants.build();
        Array valueList = new Array(
                "%" + namePrefix + "_value_list",
                type,
                valueListElements.stream()
                        .map(constant -> (Value) constant.result())
                        .toList(),
                valueListElements.stream()
                        .map(Constant::attributes)
                        .toList());
        In in = new In(
                "%" + namePrefix + "_in",
                input.result(),
                valueList.result(),
                ImmutableList.of(input.attributes(), valueList.attributes()));
        Return returnOperation = new Return("%" + namePrefix + "_return", in.result(), in.attributes());
        return new Block(
                Optional.of("^" + namePrefix),
                ImmutableList.of(parameter),
                ImmutableList.<Operation>builder()
                        .add(input)
                        .addAll(valueListElements)
                        .add(valueList)
                        .add(in)
                        .add(returnOperation)
                        .build());
    }

    private static Block predicateContaining(Operation operation)
    {
        Block.Parameter parameter = new Block.Parameter("%predicate_parameter", irType(anonymousRow(BIGINT)));
        return booleanPredicate(parameter, ImmutableList.of(operation));
    }

    private static Block booleanPredicate(Block.Parameter parameter, List<Operation> operations)
    {
        Constant trueConstant = new Constant("%predicate_true", BOOLEAN, true);
        Return returnOperation = new Return("%predicate_return", trueConstant.result(), trueConstant.attributes());
        return new Block(
                Optional.of("^predicate"),
                ImmutableList.of(parameter),
                ImmutableList.<Operation>builder()
                        .addAll(operations)
                        .add(trueConstant)
                        .add(returnOperation)
                        .build());
    }

    private static ProgramAndSource programWithPredicate(Block predicate)
    {
        RowType sourceRowType = (RowType) trinoType(predicate.parameters().getFirst().type());
        ImmutableList.Builder<Constant> sourceConstants = ImmutableList.builder();
        for (int index = 0; index < sourceRowType.getFields().size(); index++) {
            sourceConstants.add(new Constant(
                    "%source_constant_" + index,
                    sourceRowType.getFields().get(index).getType(),
                    null));
        }
        List<Constant> constants = sourceConstants.build();
        Row sourceRow = new Row(
                "%source_row",
                constants.stream()
                        .map(constant -> (Value) constant.result())
                        .toList(),
                constants.stream()
                        .map(Constant::attributes)
                        .toList());
        Return sourceReturn = new Return("%source_return", sourceRow.result(), sourceRow.attributes());
        Values source = new Values(
                "%source",
                sourceRowType,
                ImmutableList.of(new Block(
                        Optional.of("^source_row"),
                        ImmutableList.of(),
                        ImmutableList.<Operation>builder()
                                .addAll(constants)
                                .add(sourceRow)
                                .add(sourceReturn)
                                .build())));

        Filter filter = new Filter("%filter", source.result(), predicate, source.attributes());

        Block.Parameter outputParameter = new Block.Parameter("%output_parameter", irType(sourceRowType));
        FieldReference outputField = new FieldReference("%output_field", outputParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row outputRow = new Row(
                "%output_row",
                ImmutableList.of(outputField.result()),
                ImmutableList.of(outputField.attributes()));
        Return outputReturn = new Return("%output_return", outputRow.result(), outputRow.attributes());
        Output output = new Output(
                "%output",
                filter.result(),
                new Block(
                        Optional.of("^output"),
                        ImmutableList.of(outputParameter),
                        ImmutableList.of(outputField, outputRow, outputReturn)),
                ImmutableList.of("value"),
                filter.attributes());

        Program program = new Program(new Query(
                "%query",
                new Block(
                        Optional.of("^query"),
                        ImmutableList.of(),
                        ImmutableList.of(source, filter, output))));
        return new ProgramAndSource(program, source);
    }

    private static Block foldPredicate(Block predicate)
    {
        Query query = (Query) foldConstantInLists(programWithPredicate(predicate).program()).root();
        return ((Filter) query.query().operations().get(1)).predicate();
    }

    private static ConstantValue arrayConstant(Type elementType, List<?> values)
    {
        BlockBuilder builder = elementType.createBlockBuilder(null, values.size());
        values.forEach(value -> writeNativeValue(elementType, builder, value));
        return ConstantValue.of(new ArrayType(elementType), builder.build());
    }

    private record ProgramAndSource(Program program, Values source) {}
}
