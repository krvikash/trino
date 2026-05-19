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
import io.trino.spi.TrinoException;
import io.trino.spi.type.FunctionType;
import io.trino.spi.type.RowType;
import io.trino.sql.dialect.trino.operationmetadata.LambdaOperationMetadata;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.FormatOptions.PrintOptions;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.spi.StandardErrorCode.IR_ERROR;
import static io.trino.spi.type.EmptyRowType.EMPTY_ROW;
import static io.trino.sql.dialect.trino.TrinoDialect.TRINO;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static io.trino.sql.dialect.trino.operationmetadata.LambdaOperationMetadata.NAME;
import static io.trino.sql.newir.Region.singleBlockRegion;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class Lambda
        extends TrinoOperation
{
    private final Result result;
    private final Region lambda;
    private final Map<AttributeKey, Object> attributes;

    public Lambda(String resultName, Block lambda)
    {
        this(resultName, lambda, ImmutableMap.of());
    }

    public Lambda(String resultName, Block lambda, Map<AttributeKey, Object> enforcedAttributes)
    {
        super(TRINO, NAME);
        requireNonNull(resultName, "resultName is null");
        requireNonNull(lambda, "lambda is null");
        requireNonNull(enforcedAttributes, "enforcedAttributes is null");

        if (lambda.parameters().size() != 1 ||
                !(trinoType(lambda.parameters().getFirst().type()) instanceof RowType ||
                        trinoType(lambda.parameters().getFirst().type()).equals(EMPTY_ROW))) {
            throw new TrinoException(IR_ERROR, format("invalid argument type for lambda: %s. expected RowType or EmptyRowType", trinoType(lambda.parameters().getFirst().type()).getDisplayName()));
        }
        this.lambda = singleBlockRegion(lambda);

        FunctionType resultType = new FunctionType(
                trinoType(lambda.parameters().getFirst().type()).getTypeParameters(),
                trinoType(lambda.getReturnedType()));

        this.result = new Result(resultName, irType(resultType));

        ImmutableMap.Builder<AttributeKey, Object> attributes = ImmutableMap.builder();
        attributes.putAll(LambdaOperationMetadata.deriveAttributes(ImmutableMap.of(), ImmutableList.of(lambda.getTerminalOperation().attributes())));
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
        return ImmutableList.of();
    }

    @Override
    public List<Region> regions()
    {
        return ImmutableList.of(lambda);
    }

    @Override
    public Map<AttributeKey, Object> attributes()
    {
        return attributes;
    }

    @Override
    public String prettyPrint(int indentLevel, PrintOptions printOptions)
    {
        return "lambda :)";
    }

    @Override
    public Operation withRegions(List<Region> newRegions)
    {
        checkArgument(newRegions.size() == 1, "regions lists size mismatch");
        return new Lambda(
                result.name(),
                getOnlyElement(newRegions).getOnlyBlock());
    }

    @Override
    public Operation withResultName(String newName)
    {
        return new Lambda(newName, lambda.getOnlyBlock());
    }

    @Override
    public Map<AttributeKey, Object> operationAttributes()
    {
        return ImmutableMap.of();
    }

    public Block lambdaBody()
    {
        return lambda.getOnlyBlock();
    }

    @Override
    public <R, C> R accept(TrinoOperationVisitor<R, C> visitor, C context)
    {
        return visitor.visitLambda(this, context);
    }
}
