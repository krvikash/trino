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
import io.trino.spi.type.MultisetType;
import io.trino.spi.type.RowType;
import io.trino.sql.dialect.ir.IrDialect.FunctionType;
import io.trino.sql.dialect.trino.TrinoDialect;
import io.trino.sql.dialect.trino.operation.Constant;
import io.trino.sql.dialect.trino.operation.Values;
import io.trino.sql.dialect.trino.operationmetadata.CastOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.CoalesceOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.FieldReferenceOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.ProjectOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.ReturnOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.RowOperationMetadata;
import io.trino.sql.newir.Attributes;
import io.trino.sql.newir.Operation.OperationId;
import io.trino.sql.newir.Type;
import io.trino.sql.planner.exploratory.TestingMemoBuilder.ChildSpecification;
import io.trino.sql.planner.exploratory.TestingMemoBuilder.OperationSpecification;

import java.util.Collections;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.EmptyRowType.EMPTY_ROW;
import static io.trino.sql.dialect.ir.IrAttributeUtils.deterministic;
import static io.trino.sql.dialect.ir.IrAttributeUtils.safe;
import static io.trino.sql.dialect.ir.IrAttributeUtils.terminalOperation;
import static io.trino.sql.dialect.ir.IrDialect.DEFAULT_BLOCK_PARAMETER_ATTRIBUTES;
import static io.trino.sql.dialect.ir.IrDialect.IR;
import static io.trino.sql.dialect.trino.RelationalProgramBuilder.relationRowType;
import static io.trino.sql.dialect.trino.TrinoDialect.TRINO;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static io.trino.sql.dialect.trino.operationmetadata.CastOperationMetadata.TO_TYPE;
import static io.trino.sql.dialect.trino.operationmetadata.FieldReferenceOperationMetadata.FIELD_INDEX;
import static io.trino.sql.planner.exploratory.TestingMemoBuilder.OperationSpecification.operation;
import static io.trino.sql.planner.exploratory.TestingMemoBuilder.parameter;

/**
 * Builds trino {@link OperationSpecification}s for rule tests.
 */
public final class TrinoMemoOperations
{
    private TrinoMemoOperations() {}

    /**
     * A {@code trino.cast} from {@code fromType} to {@code toType}.
     */
    public static OperationSpecification cast(Type fromType, Type toType, List<Type> groupParameterTypes, ChildSpecification input)
    {
        return operation(
                TRINO,
                new OperationId(CastOperationMetadata.NAME, ImmutableList.of(fromType), ImmutableList.of()),
                toType)
                .groupParameterTypes(groupParameterTypes)
                .children(input)
                .attributes(TO_TYPE.asAttributes(trinoType(toType)));
    }

    /**
     * A {@code trino.coalesce} over {@code children} (all {@code bigint}), producing {@code bigint}.
     */
    public static OperationSpecification coalesce(List<Type> groupParameterTypes, Attributes attributes, ChildSpecification... children)
    {
        return operation(
                TRINO,
                new OperationId(CoalesceOperationMetadata.NAME, ImmutableList.copyOf(Collections.nCopies(children.length, bigint())), ImmutableList.of()),
                bigint())
                .groupParameterTypes(groupParameterTypes)
                .children(children)
                .attributes(attributes);
    }

    /**
     * A {@code trino.constant}.
     */
    public static OperationSpecification constant(Type... groupParameterTypes)
    {
        return constant(ImmutableList.copyOf(groupParameterTypes));
    }

    public static OperationSpecification constant(List<Type> groupParameterTypes)
    {
        Constant constant = new Constant("%constant", BIGINT, 0L);
        return operation(constant.dialect(), constant.id(), constant.result().type())
                .groupParameterTypes(groupParameterTypes)
                .attributes(constant.attributes());
    }

    /**
     * A {@code trino.field_reference} reading field {@code fieldIndex} of the only parameter.
     */
    public static OperationSpecification fieldReference(Type parameterType, int fieldIndex)
    {
        return fieldReference(ImmutableList.of(parameterType), 0, fieldIndex);
    }

    /**
     * A {@code trino.field_reference} reading field {@code fieldIndex} of the parameter at {@code parameterIndex}.
     */
    public static OperationSpecification fieldReference(List<Type> groupParameterTypes, int parameterIndex, int fieldIndex)
    {
        checkArgument(parameterIndex >= 0 && parameterIndex < groupParameterTypes.size(), "parameterIndex out of bounds");

        Type parameterType = groupParameterTypes.get(parameterIndex);
        Attributes operationAttributes = FIELD_INDEX.asAttributes(fieldIndex);
        Attributes attributes = Attributes.builder()
                .putAll(operationAttributes)
                .putAll(FieldReferenceOperationMetadata.deriveAttributes(operationAttributes, ImmutableList.of(DEFAULT_BLOCK_PARAMETER_ATTRIBUTES)))
                .buildOrThrow();
        return operation(
                TRINO,
                new OperationId(FieldReferenceOperationMetadata.NAME, ImmutableList.of(parameterType), ImmutableList.of()),
                fieldType(parameterType, fieldIndex))
                .groupParameterTypes(groupParameterTypes)
                .children(parameter(parameterIndex))
                .attributes(attributes);
    }

    /**
     * A {@code trino.project} of {@code inputRelationType} producing {@code outputRowType}, with an input child
     * and an assignments lambda child. The assignments region takes a single proper parameter (the input row).
     */
    public static OperationSpecification project(Type inputRelationType, Type outputRowType, List<Type> groupParameterTypes, ChildSpecification input, ChildSpecification assignments)
    {
        Type properParameterType = irType(relationRowType(trinoType(inputRelationType)));
        return projectWithRegionArguments(inputRelationType, outputRowType, groupParameterTypes, ImmutableList.of(properParameterType), input, assignments);
    }

    /**
     * A {@code trino.project} whose assignments region takes the given {@code regionArgumentTypes} (rather than
     * the canonical single proper parameter). Used to build deliberately non-canonical assignment shapes.
     */
    public static OperationSpecification projectWithRegionArguments(Type inputRelationType, Type outputRowType, List<Type> groupParameterTypes, List<Type> regionArgumentTypes, ChildSpecification input, ChildSpecification assignments)
    {
        Type assignmentsRegionType = new Type(IR, new FunctionType(regionArgumentTypes, outputRowType));
        return operation(
                TRINO,
                new OperationId(ProjectOperationMetadata.NAME, ImmutableList.of(inputRelationType), ImmutableList.of(assignmentsRegionType)),
                relationType(outputRowType))
                .groupParameterTypes(groupParameterTypes)
                .children(input, assignments);
    }

    /**
     * A {@code trino.return}.
     */
    public static OperationSpecification returnOperation(Type type, List<Type> groupParameterTypes, ChildSpecification child)
    {
        return operation(
                TRINO,
                new OperationId(ReturnOperationMetadata.NAME, ImmutableList.of(type), ImmutableList.of()),
                type)
                .groupParameterTypes(groupParameterTypes)
                .children(child)
                .attributes(terminalOperation());
    }

    /**
     * A {@code trino.row} producing the given {@code outputRowType}.
     */
    public static OperationSpecification row(Type outputRowType, List<Type> groupParameterTypes, ChildSpecification... children)
    {
        List<Type> fieldTypes = fieldTypes(outputRowType);
        checkArgument(children.length == fieldTypes.size(), "row has %s fields but %s children", fieldTypes.size(), children.length);
        return operation(
                TRINO,
                new OperationId(RowOperationMetadata.NAME, fieldTypes, ImmutableList.of()),
                outputRowType)
                .groupParameterTypes(groupParameterTypes)
                .children(children);
    }

    /**
     * A {@code trino.values} leaf producing zero rows of the given {@code outputRow} type.
     */
    public static OperationSpecification values(Type outputRow)
    {
        return values(outputRow, ImmutableList.of());
    }

    public static OperationSpecification values(Type outputRow, List<Type> groupParameterTypes)
    {
        Values values = new Values("%values", (RowType) trinoType(outputRow), ImmutableList.of());
        return operation(values.dialect(), values.id(), values.result().type())
                .groupParameterTypes(groupParameterTypes)
                .attributes(values.attributes());
    }

    /**
     * A {@code trino.values} leaf producing zero rows of EMPTY_ROW type (no fields).
     */
    public static OperationSpecification valuesWithoutFields()
    {
        return valuesWithoutFields(ImmutableList.of());
    }

    public static OperationSpecification valuesWithoutFields(List<Type> groupParameterTypes)
    {
        Values values = Values.valuesWithoutFields("%values", 0);
        return operation(values.dialect(), values.id(), values.result().type())
                .groupParameterTypes(groupParameterTypes)
                .attributes(values.attributes());
    }

    // Type helpers for building operation argument, region and result types.

    public static Type bigint()
    {
        return irType(BIGINT);
    }

    public static Type rowType(Type... fields)
    {
        if (fields.length == 0) {
            return irType(EMPTY_ROW);
        }
        return irType(RowType.anonymous(ImmutableList.copyOf(fields).stream()
                .map(TrinoDialect::trinoType)
                .collect(toImmutableList())));
    }

    public static Type relationType(Type row)
    {
        if (trinoType(row).equals(EMPTY_ROW)) {
            return irType(new MultisetType(EMPTY_ROW));
        }
        return irType(new MultisetType(trinoType(row)));
    }

    private static Type fieldType(Type rowType, int fieldIndex)
    {
        checkArgument(trinoType(rowType) instanceof RowType || trinoType(rowType).equals(EMPTY_ROW), "rowType must be a RowType or EMPTY_ROW");

        return irType(trinoType(rowType).getTypeParameters().get(fieldIndex));
    }

    private static List<Type> fieldTypes(Type rowType)
    {
        checkArgument(trinoType(rowType) instanceof RowType || trinoType(rowType).equals(EMPTY_ROW), "rowType must be a RowType or EMPTY_ROW");

        return trinoType(rowType).getTypeParameters().stream()
                .map(TrinoDialect::irType)
                .collect(toImmutableList());
    }

    // Attribute profiles for expressions whose attributes are set explicitly.

    public static Attributes safeAndDeterministic()
    {
        Attributes.Builder attributes = Attributes.builder();
        safe(attributes);
        deterministic(attributes);
        return attributes.buildOrThrow();
    }

    public static Attributes safeUnknownDeterminism()
    {
        Attributes.Builder attributes = Attributes.builder();
        safe(attributes);
        return attributes.buildOrThrow();
    }
}
