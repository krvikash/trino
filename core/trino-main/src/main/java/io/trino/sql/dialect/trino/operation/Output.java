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
package io.trino.sql.dialect.trino.operation;

import com.google.common.collect.ImmutableList;
import io.trino.spi.TrinoException;
import io.trino.spi.type.RowType;
import io.trino.sql.dialect.trino.operationmetadata.OutputOperationMetadata;
import io.trino.sql.newir.Attributes;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.FormatOptions.PrintOptions;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;

import java.util.List;

import static io.trino.spi.StandardErrorCode.IR_ERROR;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.EmptyRowType.EMPTY_ROW;
import static io.trino.sql.dialect.ir.IrAttributeUtils.terminalOperation;
import static io.trino.sql.dialect.trino.RelationalProgramBuilder.relationRowType;
import static io.trino.sql.dialect.trino.TrinoDialect.TRINO;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static io.trino.sql.dialect.trino.TypeConstraint.IS_RELATION;
import static io.trino.sql.dialect.trino.operationmetadata.OutputOperationMetadata.COLUMN_NAMES;
import static io.trino.sql.dialect.trino.operationmetadata.OutputOperationMetadata.NAME;
import static io.trino.sql.newir.Region.singleBlockRegion;
import static java.util.Objects.requireNonNull;

public final class Output
        extends TrinoOperation
{
    private final Result result;
    private final Value input;
    private final Region fieldSelector;
    private final Attributes attributes;

    public Output(String resultName, Value input, Block fieldSelector, List<String> outputNames, Attributes sourceAttributes)
    {
        this(resultName, input, fieldSelector, outputNames, sourceAttributes, Attributes.empty());
    }

    public Output(String resultName, Value input, Block fieldSelector, List<String> outputNames, Attributes sourceAttributes, Attributes enforcedAttributes)
    {
        super(TRINO, NAME);
        requireNonNull(resultName, "resultName is null");
        requireNonNull(input, "input is null");
        requireNonNull(fieldSelector, "fieldSelector is null");
        requireNonNull(outputNames, "outputNames is null");
        requireNonNull(sourceAttributes, "sourceAttributes is null");
        requireNonNull(enforcedAttributes, "enforcedAttributes is null");

        if (!IS_RELATION.test(trinoType(input.type()))) {
            throw new TrinoException(IR_ERROR, "input to the Output operation must be of relation type");
        }

        this.result = new Result(resultName, irType(BOOLEAN)); // returning boolean to avoid introducing type void

        this.input = input;

        if (fieldSelector.parameters().size() != 1 ||
                !trinoType(fieldSelector.parameters().getFirst().type()).equals(relationRowType(trinoType(input.type()))) ||
                trinoType(fieldSelector.parameters().getFirst().type()).equals(EMPTY_ROW) ||
                !(trinoType(fieldSelector.getReturnedType()) instanceof RowType) ||
                ((RowType) trinoType(fieldSelector.getReturnedType())).getTypeParameters().size() != outputNames.size()) {
            throw new TrinoException(IR_ERROR, "invalid field selection for Output operation");
        }

        this.fieldSelector = singleBlockRegion(fieldSelector);

        Attributes.Builder operationAttributesBuilder = Attributes.builder();
        COLUMN_NAMES.putAttribute(operationAttributesBuilder, outputNames);
        terminalOperation(operationAttributesBuilder);
        Attributes operationAttributes = operationAttributesBuilder.buildOrThrow();

        Attributes.Builder attributes = Attributes.builder();
        attributes.putAll(operationAttributes);
        attributes.putAll(OutputOperationMetadata.deriveAttributes(operationAttributes, ImmutableList.of(sourceAttributes, fieldSelector.getTerminalOperation().attributes())));

        // TODO check if new attributes are compatible with existing ones. In particular, internal attributes must not change
        attributes.putAll(enforcedAttributes);
        this.attributes = attributes.buildKeepingLast();
    }

    @Override
    public Result result()
    {
        return result;
    }

    @Override
    public List<Value> arguments()
    {
        return ImmutableList.of(input);
    }

    @Override
    public List<Region> regions()
    {
        return ImmutableList.of(fieldSelector);
    }

    @Override
    public Attributes attributes()
    {
        return attributes;
    }

    @Override
    public String prettyPrint(int indentLevel, PrintOptions printOptions)
    {
        return "pretty output";
    }

    @Override
    public Operation withArgument(Value newArgument, int index)
    {
        validateArgument(newArgument, index);
        return new Output(
                result.name(),
                newArgument,
                fieldSelector.getOnlyBlock(),
                COLUMN_NAMES.getAttribute(attributes),
                Attributes.empty());
    }

    @Override
    public Attributes operationAttributes()
    {
        return filterAttributes(OutputOperationMetadata.OPERATION_ATTRIBUTES);
    }

    public Block outputFieldSelector()
    {
        return fieldSelector.getOnlyBlock();
    }

    @Override
    public <R, C> R accept(TrinoOperationVisitor<R, C> visitor, C context)
    {
        return visitor.visitOutput(this, context);
    }
}
