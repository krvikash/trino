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
import io.trino.sql.dialect.trino.operationmetadata.ExplainAnalyzeOperationMetadata;
import io.trino.sql.newir.Attributes;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.FormatOptions.PrintOptions;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;

import java.util.List;

import static io.trino.spi.StandardErrorCode.IR_ERROR;
import static io.trino.spi.type.RowType.anonymousRow;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.dialect.trino.OperationValidationUtils.validateRowSelector;
import static io.trino.sql.dialect.trino.RelationalProgramBuilder.relationRowType;
import static io.trino.sql.dialect.trino.TrinoDialect.TRINO;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static io.trino.sql.dialect.trino.TypeConstraint.IS_RELATION;
import static io.trino.sql.dialect.trino.operationmetadata.ExplainAnalyzeOperationMetadata.NAME;
import static io.trino.sql.dialect.trino.operationmetadata.ExplainAnalyzeOperationMetadata.VERBOSE;
import static io.trino.sql.newir.Region.singleBlockRegion;
import static java.util.Objects.requireNonNull;

public class ExplainAnalyze
        extends TrinoOperation
{
    private final Result result;
    private final Value input;
    private final Region fieldSelector;
    private final Attributes attributes;

    public ExplainAnalyze(String resultName, Value input, Block fieldSelector, boolean verbose, Attributes sourceAttributes)
    {
        this(resultName, input, fieldSelector, verbose, sourceAttributes, Attributes.empty());
    }

    public ExplainAnalyze(String resultName, Value input, Block fieldSelector, boolean verbose, Attributes sourceAttributes, Attributes enforcedAttributes)
    {
        super(TRINO, NAME);
        requireNonNull(resultName, "resultName is null");
        requireNonNull(input, "input is null");
        requireNonNull(fieldSelector, "fieldSelector is null");
        requireNonNull(sourceAttributes, "sourceAttributes is null");
        requireNonNull(enforcedAttributes, "enforcedAttributes is null");

        if (!IS_RELATION.test(trinoType(input.type()))) {
            throw new TrinoException(IR_ERROR, "input to the ExplainAnalyze operation must be of relation type");
        }
        this.input = input;

        this.result = new Result(resultName, irType(new MultisetType(anonymousRow(VARCHAR))));

        validateRowSelector(fieldSelector, relationRowType(trinoType(input.type())), "invalid field selection for ExplainAnalyze operation");
        this.fieldSelector = singleBlockRegion(fieldSelector);

        Attributes operationAttributes = VERBOSE.asAttributes(verbose);

        Attributes.Builder attributes = Attributes.builder();
        attributes.putAll(operationAttributes);
        attributes.putAll(ExplainAnalyzeOperationMetadata.deriveAttributes(operationAttributes, ImmutableList.of(sourceAttributes, fieldSelector.getTerminalOperation().attributes())));

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
        return "pretty explain analyze";
    }

    @Override
    public Operation withArgument(Value newArgument, int index)
    {
        validateArgument(newArgument, index);
        return new ExplainAnalyze(
                result.name(),
                newArgument,
                fieldSelector.getOnlyBlock(),
                VERBOSE.getAttribute(attributes),
                Attributes.empty());
    }

    @Override
    public Attributes operationAttributes()
    {
        return filterAttributes(ExplainAnalyzeOperationMetadata.OPERATION_ATTRIBUTES);
    }

    public Block fieldSelector()
    {
        return fieldSelector.getOnlyBlock();
    }

    @Override
    public <R, C> R accept(TrinoOperationVisitor<R, C> visitor, C context)
    {
        return visitor.visitExplainAnalyze(this, context);
    }
}
