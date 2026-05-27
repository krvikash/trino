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
import io.trino.sql.dialect.trino.operationmetadata.LetOperationMetadata;
import io.trino.sql.newir.Attributes;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.FormatOptions.PrintOptions;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.spi.StandardErrorCode.IR_ERROR;
import static io.trino.sql.dialect.trino.TrinoDialect.TRINO;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static io.trino.sql.dialect.trino.operationmetadata.LetOperationMetadata.NAME;
import static io.trino.sql.newir.Region.singleBlockRegion;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Binds a value once and evaluates the body block with the bound value available
 * as the single field of the block parameter. This is the 1-1 counterpart of the
 * old IR {@code Let} expression.
 */
public final class Let
        extends TrinoOperation
{
    private final Result result;
    private final Value value;
    private final Region body;
    private final Attributes attributes;

    public Let(String resultName, Value value, Block body, Attributes sourceAttributes)
    {
        this(resultName, value, body, sourceAttributes, Attributes.empty());
    }

    public Let(String resultName, Value value, Block body, Attributes sourceAttributes, Attributes enforcedAttributes)
    {
        super(TRINO, NAME);
        requireNonNull(resultName, "resultName is null");
        requireNonNull(value, "value is null");
        requireNonNull(body, "body is null");
        requireNonNull(sourceAttributes, "sourceAttributes is null");
        requireNonNull(enforcedAttributes, "enforcedAttributes is null");

        if (body.parameters().size() != 1 ||
                !(trinoType(body.parameters().getFirst().type()) instanceof RowType parameterType) ||
                parameterType.getFields().size() != 1 ||
                !parameterType.getFields().getFirst().getType().equals(trinoType(value.type()))) {
            throw new TrinoException(IR_ERROR, format("invalid body parameter for let operation. expected a row with a single field of type %s", trinoType(value.type()).getDisplayName()));
        }

        this.result = new Result(resultName, body.getReturnedType());
        this.value = value;
        this.body = singleBlockRegion(body);

        Attributes.Builder attributes = Attributes.builder();
        attributes.putAll(LetOperationMetadata.deriveAttributes(Attributes.empty(), ImmutableList.of(sourceAttributes, body.getTerminalOperation().attributes())));
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
        return ImmutableList.of(value);
    }

    @Override
    public List<Region> regions()
    {
        return ImmutableList.of(body);
    }

    @Override
    public Attributes attributes()
    {
        return attributes;
    }

    @Override
    public String prettyPrint(int indentLevel, PrintOptions printOptions)
    {
        return "pretty let";
    }

    @Override
    public Operation withArgument(Value newArgument, int index)
    {
        validateArgument(newArgument, index);
        return new Let(
                result.name(),
                newArgument,
                body.getOnlyBlock(),
                Attributes.empty());
    }

    @Override
    public Operation withRegions(List<Region> newRegions)
    {
        checkArgument(newRegions.size() == 1, "regions lists size mismatch");
        return new Let(
                result.name(),
                value,
                getOnlyElement(newRegions).getOnlyBlock(),
                Attributes.empty());
    }

    @Override
    public Operation withResultName(String newName)
    {
        return new Let(newName, value, body.getOnlyBlock(), Attributes.empty());
    }

    @Override
    public Attributes operationAttributes()
    {
        return Attributes.empty();
    }

    public Value value()
    {
        return value;
    }

    public Block body()
    {
        return body.getOnlyBlock();
    }

    @Override
    public <R, C> R accept(TrinoOperationVisitor<R, C> visitor, C context)
    {
        return visitor.visitLet(this, context);
    }
}
