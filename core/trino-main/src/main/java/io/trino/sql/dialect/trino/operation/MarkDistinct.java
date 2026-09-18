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
import io.trino.spi.type.MultisetType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.dialect.trino.operationmetadata.MarkDistinctOperationMetadata;
import io.trino.sql.newir.Attributes;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.FormatOptions.PrintOptions;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;

import java.util.List;

import static io.trino.spi.StandardErrorCode.IR_ERROR;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.dialect.trino.OperationValidationUtils.validateNonEmptyRowSelector;
import static io.trino.sql.dialect.trino.RelationalProgramBuilder.relationRowType;
import static io.trino.sql.dialect.trino.TrinoDialect.TRINO;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static io.trino.sql.dialect.trino.TypeConstraint.IS_RELATION;
import static io.trino.sql.dialect.trino.operationmetadata.MarkDistinctOperationMetadata.NAME;
import static io.trino.sql.newir.Region.singleBlockRegion;
import static java.util.Objects.requireNonNull;

public final class MarkDistinct
        extends TrinoOperation
{
    private final Result result;
    private final Value source;
    private final Region distinctFieldsSelector;
    private final Attributes attributes;

    public MarkDistinct(String resultName, Value source, Block distinctFieldsSelector, Attributes sourceAttributes)
    {
        this(resultName, source, distinctFieldsSelector, sourceAttributes, Attributes.empty());
    }

    public MarkDistinct(String resultName, Value source, Block distinctFieldsSelector, Attributes sourceAttributes, Attributes enforcedAttributes)
    {
        super(TRINO, NAME);
        requireNonNull(resultName, "resultName is null");
        requireNonNull(source, "source is null");
        requireNonNull(distinctFieldsSelector, "distinctFieldsSelector is null");
        requireNonNull(sourceAttributes, "sourceAttributes is null");
        requireNonNull(enforcedAttributes, "enforcedAttributes is null");

        if (!IS_RELATION.test(trinoType(source.type()))) {
            throw new TrinoException(IR_ERROR, "source of MarkDistinct operation must be of relation type");
        }
        this.source = source;

        validateNonEmptyRowSelector(distinctFieldsSelector, relationRowType(trinoType(source.type())), "invalid distinct fields selector for MarkDistinct operation");
        this.distinctFieldsSelector = singleBlockRegion(distinctFieldsSelector);

        List<Type> outputTypes = ImmutableList.<Type>builder()
                .addAll(relationRowType(trinoType(source.type())).getTypeParameters())
                .add(BOOLEAN)
                .build();
        this.result = new Result(resultName, irType(new MultisetType(RowType.anonymous(outputTypes))));

        Attributes.Builder attributes = Attributes.builder();
        attributes.putAll(MarkDistinctOperationMetadata.deriveAttributes(
                Attributes.empty(),
                ImmutableList.of(sourceAttributes, distinctFieldsSelector.getTerminalOperation().attributes())));
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
        return ImmutableList.of(source);
    }

    @Override
    public List<Region> regions()
    {
        return ImmutableList.of(distinctFieldsSelector);
    }

    @Override
    public Attributes attributes()
    {
        return attributes;
    }

    @Override
    public String prettyPrint(int indentLevel, PrintOptions printOptions)
    {
        return "pretty mark distinct";
    }

    @Override
    public Operation withArgument(Value newArgument, int index)
    {
        validateArgument(newArgument, index);
        return new MarkDistinct(
                result.name(),
                newArgument,
                distinctFieldsSelector.getOnlyBlock(),
                Attributes.empty());
    }

    @Override
    public Attributes operationAttributes()
    {
        return Attributes.empty();
    }

    public Value source()
    {
        return source;
    }

    public Block distinctFieldsSelector()
    {
        return distinctFieldsSelector.getOnlyBlock();
    }

    @Override
    public <R, C> R accept(TrinoOperationVisitor<R, C> visitor, C context)
    {
        return visitor.visitMarkDistinct(this, context);
    }
}
