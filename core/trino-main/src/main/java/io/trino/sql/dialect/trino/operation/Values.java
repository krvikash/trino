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
import io.trino.spi.type.MultisetType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.dialect.trino.operationmetadata.ValuesOperationMetadata;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.FormatOptions.PrintOptions;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;

import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.StandardErrorCode.IR_ERROR;
import static io.trino.spi.type.EmptyRowType.EMPTY_ROW;
import static io.trino.sql.dialect.trino.TrinoDialect.TRINO;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static io.trino.sql.dialect.trino.operationmetadata.ValuesOperationMetadata.CARDINALITY;
import static io.trino.sql.dialect.trino.operationmetadata.ValuesOperationMetadata.NAME;
import static io.trino.sql.dialect.trino.operationmetadata.ValuesOperationMetadata.ROW_TYPE;
import static io.trino.sql.newir.Region.singleBlockRegion;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class Values
        extends TrinoOperation
{
    private final Result result;
    private final List<Region> rows;
    private final Map<AttributeKey, Object> attributes;

    // TODO Values can be constant or correlated. If it is constant, it should be folded to Constant operation

    public Values(String resultName, RowType rowType, List<Block> rows)
    {
        this(resultName, rowType, rows, ImmutableMap.of());
    }

    public Values(String resultName, RowType rowType, List<Block> rows, Map<AttributeKey, Object> enforcedAttributes)
    {
        super(TRINO, NAME);
        requireNonNull(resultName, "resultName is null");
        requireNonNull(rowType, "rowType is null");
        requireNonNull(rows, "rows is null");
        requireNonNull(enforcedAttributes, "enforcedAttributes is null");

        // Create output type with anonymous fields.
        // This is consistent with the Trino behavior in StatementAnalyzer: the RelationType
        // for Values has anonymous fields even if individual rows had named fields.
        RowType outputType = RowType.anonymous(rowType.getTypeParameters());
        this.result = new Result(resultName, irType(new MultisetType(outputType)));

        // Verify that each row matches the output type. Check field types only.
        // Field names are ignored. They will be overridden by the output type.
        this.rows = rows.stream()
                .map(rowBlock -> {
                    // verify that blocks have no parameters
                    if (!rowBlock.parameters().isEmpty()) {
                        throw new TrinoException(IR_ERROR, format("no block parameters expected. got %s parameters", rowBlock.parameters().size()));
                    }
                    Type blockType = trinoType(rowBlock.getReturnedType());
                    if (!(blockType instanceof RowType returnedRowType)) {
                        throw new TrinoException(IR_ERROR, "block should return RowType. actual: " + blockType.getDisplayName());
                    }
                    if (!returnedRowType.getTypeParameters().equals(rowType.getTypeParameters())) {
                        throw new TrinoException(IR_ERROR, format("type of row: %s does not match the declared output type: %s", blockType.getDisplayName(), rowType.getDisplayName()));
                    }
                    return singleBlockRegion(rowBlock);
                })
                .collect(toImmutableList());
        // TODO all Blocks representing rows could be combined into one Block returning a multiset<Row>

        ImmutableMap.Builder<AttributeKey, Object> operationAttributesBuilder = ImmutableMap.builder();
        CARDINALITY.putAttribute(operationAttributesBuilder, (long) rows.size());
        ROW_TYPE.putAttribute(operationAttributesBuilder, outputType);
        Map<AttributeKey, Object> operationAttributes = operationAttributesBuilder.buildOrThrow();

        ImmutableMap.Builder<AttributeKey, Object> attributes = ImmutableMap.builder();
        attributes.putAll(operationAttributes);
        attributes.putAll(ValuesOperationMetadata.deriveAttributes(operationAttributes, rows.stream().map(Block::getTerminalOperation).map(Operation::attributes).collect(toImmutableList())));

        // TODO check if new attributes are compatible with existing ones. In particular, internal attributes must not change
        attributes.putAll(enforcedAttributes);
        this.attributes = attributes.buildKeepingLast();
    }

    private Values(String resultName, int rows, Map<AttributeKey, Object> enforcedAttributes)
    {
        super(TRINO, NAME);
        requireNonNull(resultName, "resultName is null");
        requireNonNull(enforcedAttributes, "enforcedAttributes is null");

        if (rows < 0) {
            throw new TrinoException(IR_ERROR, "negative row count: " + rows);
        }

        this.result = new Result(resultName, irType(new MultisetType(EMPTY_ROW)));

        this.rows = ImmutableList.of();

        ImmutableMap.Builder<AttributeKey, Object> operationAttributesBuilder = ImmutableMap.builder();
        CARDINALITY.putAttribute(operationAttributesBuilder, (long) rows);
        ROW_TYPE.putAttribute(operationAttributesBuilder, EMPTY_ROW);
        Map<AttributeKey, Object> operationAttributes = operationAttributesBuilder.buildOrThrow();

        ImmutableMap.Builder<AttributeKey, Object> attributes = ImmutableMap.builder();
        attributes.putAll(operationAttributes);
        attributes.putAll(ValuesOperationMetadata.deriveAttributes(operationAttributes, ImmutableList.of()));

        // TODO check if new attributes are compatible with existing ones. In particular, internal attributes must not change
        attributes.putAll(enforcedAttributes);
        this.attributes = attributes.buildKeepingLast();
    }

    public static Values valuesWithoutFields(String resultName, int rows)
    {
        return new Values(resultName, rows, ImmutableMap.of());
    }

    public static Values valuesWithoutFields(String resultName, int rows, Map<AttributeKey, Object> enforcedAttributes)
    {
        return new Values(resultName, rows, enforcedAttributes);
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
        return rows;
    }

    @Override
    public Map<AttributeKey, Object> attributes()
    {
        return attributes;
    }

    @Override
    public String prettyPrint(int indentLevel, PrintOptions printOptions)
    {
        return "pretty values";
    }

    @Override
    public Map<AttributeKey, Object> operationAttributes()
    {
        return filterAttributes(ValuesOperationMetadata.OPERATION_ATTRIBUTES);
    }

    public List<Block> rows()
    {
        return rows.stream()
                .map(Region::getOnlyBlock)
                .collect(toImmutableList());
    }

    @Override
    public <R, C> R accept(TrinoOperationVisitor<R, C> visitor, C context)
    {
        return visitor.visitValues(this, context);
    }
}
