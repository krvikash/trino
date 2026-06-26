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
import io.trino.sql.dialect.trino.operationmetadata.QueryOperationMetadata;
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
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.dialect.ir.IrAttributeUtils.terminalOperation;
import static io.trino.sql.dialect.trino.TrinoDialect.TRINO;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static io.trino.sql.dialect.trino.operationmetadata.QueryOperationMetadata.NAME;
import static io.trino.sql.newir.Region.singleBlockRegion;
import static java.util.Objects.requireNonNull;

public final class Query
        extends TrinoOperation
{
    private final Result result;
    private final Region query;
    private final Attributes attributes;

    public Query(String resultName, Block query)
    {
        this(resultName, query, Attributes.empty());
    }

    public Query(String resultName, Block query, Attributes enforcedAttributes)
    {
        super(TRINO, NAME);
        requireNonNull(resultName, "resultName is null");
        requireNonNull(query, "query is null");
        requireNonNull(enforcedAttributes, "enforcedAttributes is null");

        this.result = new Result(resultName, irType(BOOLEAN)); // returning boolean to avoid introducing type void

        if (!(query.getTerminalOperation() instanceof Output)) {
            throw new TrinoException(IR_ERROR, "query block must end in Output operation");
        }
        this.query = singleBlockRegion(query);

        Attributes operationAttributes = terminalOperation();

        Attributes.Builder attributes = Attributes.builder();
        attributes.putAll(operationAttributes);
        attributes.putAll(QueryOperationMetadata.deriveAttributes(operationAttributes, ImmutableList.of(query.getTerminalOperation().attributes())));

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
        return ImmutableList.of(query);
    }

    @Override
    public Attributes attributes()
    {
        return attributes;
    }

    @Override
    public String prettyPrint(int indentLevel, PrintOptions printOptions)
    {
        return "♡♡♡ query ♡♡♡";
    }

    @Override
    public Operation withRegions(List<Region> newRegions)
    {
        checkArgument(newRegions.size() == 1, "regions lists size mismatch");
        return new Query(
                result.name(),
                getOnlyElement(newRegions).getOnlyBlock());
    }

    @Override
    public Attributes operationAttributes()
    {
        return Attributes.empty();
    }

    public Block query()
    {
        return query.getOnlyBlock();
    }
}
