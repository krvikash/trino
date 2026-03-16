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
import com.google.common.collect.ImmutableMap;
import io.trino.spi.type.MultisetType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.dialect.trino.operationmetadata.ExceptOperationMetadata;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.FormatOptions.PrintOptions;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.EmptyRowType.EMPTY_ROW;
import static io.trino.sql.dialect.trino.TrinoDialect.TRINO;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static io.trino.sql.dialect.trino.operation.SetOperations.validateSetOperation;
import static io.trino.sql.dialect.trino.operationmetadata.ExceptOperationMetadata.DISTINCT;
import static io.trino.sql.dialect.trino.operationmetadata.ExceptOperationMetadata.NAME;
import static java.util.Objects.requireNonNull;

public final class Except
        extends TrinoOperation
{
    private final Result result;
    private final List<Value> inputs;
    private final List<Region> inputFieldSelectors;
    private final Map<AttributeKey, Object> attributes;

    public Except(String resultName, List<Value> inputs, List<Block> inputFieldSelectors, boolean distinct, List<Map<AttributeKey, Object>> sourceAttributes)
    {
        this(resultName, inputs, inputFieldSelectors, distinct, sourceAttributes, ImmutableMap.of());
    }

    public Except(String resultName, List<Value> inputs, List<Block> inputFieldSelectors, boolean distinct, List<Map<AttributeKey, Object>> sourceAttributes, Map<AttributeKey, Object> enforcedAttributes)
    {
        super(TRINO, NAME);
        requireNonNull(resultName, "resultName is null");
        requireNonNull(inputs, "inputs is null");
        requireNonNull(inputFieldSelectors, "inputFieldSelectors is null");
        requireNonNull(sourceAttributes, "sourceAttributes is null");
        requireNonNull(enforcedAttributes, "enforcedAttributes is null");

        validateSetOperation(inputs, inputFieldSelectors, sourceAttributes, "Except");

        this.inputs = ImmutableList.copyOf(inputs);
        this.inputFieldSelectors = inputFieldSelectors.stream()
                .map(Region::singleBlockRegion)
                .collect(toImmutableList());

        List<Type> outputFieldTypes = trinoType(inputFieldSelectors.getFirst().getReturnedType()).getTypeParameters();
        Type outputRowType = outputFieldTypes.isEmpty() ? EMPTY_ROW : RowType.anonymous(outputFieldTypes);
        this.result = new Result(resultName, irType(new MultisetType(outputRowType)));

        ImmutableMap.Builder<AttributeKey, Object> operationAttributesBuilder = ImmutableMap.builder();
        DISTINCT.putAttribute(operationAttributesBuilder, distinct);
        Map<AttributeKey, Object> operationAttributes = operationAttributesBuilder.buildOrThrow();

        ImmutableList.Builder<Map<AttributeKey, Object>> childAttributes = ImmutableList.builder();
        childAttributes.addAll(sourceAttributes);
        inputFieldSelectors.stream()
                .map(Block::getTerminalOperation)
                .map(Operation::attributes)
                .forEach(childAttributes::add);

        ImmutableMap.Builder<AttributeKey, Object> attributes = ImmutableMap.builder();
        attributes.putAll(operationAttributes);
        attributes.putAll(ExceptOperationMetadata.deriveAttributes(operationAttributes, childAttributes.build()));
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
        return inputs;
    }

    @Override
    public List<Region> regions()
    {
        return inputFieldSelectors;
    }

    @Override
    public Map<AttributeKey, Object> attributes()
    {
        return attributes;
    }

    @Override
    public String prettyPrint(int indentLevel, PrintOptions printOptions)
    {
        return "pretty except";
    }

    @Override
    public Operation withArgument(Value newArgument, int index)
    {
        validateArgument(newArgument, index);
        List<Value> newInputs = new ArrayList<>(inputs);
        newInputs.set(index, newArgument);
        return new Except(
                result.name(),
                newInputs,
                inputFieldSelectors.stream()
                        .map(Region::getOnlyBlock)
                        .collect(toImmutableList()),
                DISTINCT.getAttribute(attributes),
                emptySourceAttributes(inputs.size()));
    }

    @Override
    public Map<AttributeKey, Object> operationAttributes()
    {
        return filterAttributes(ExceptOperationMetadata.OPERATION_ATTRIBUTES);
    }

    public List<Block> inputFieldSelectors()
    {
        return inputFieldSelectors.stream()
                .map(Region::getOnlyBlock)
                .collect(toImmutableList());
    }

    @Override
    public <R, C> R accept(TrinoOperationVisitor<R, C> visitor, C context)
    {
        return visitor.visitExcept(this, context);
    }
}
