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
import io.trino.sql.dialect.trino.operationmetadata.LogicalOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.LogicalOperationMetadata.LogicalOperator;
import io.trino.sql.newir.Attributes;
import io.trino.sql.newir.FormatOptions.PrintOptions;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;

import java.util.ArrayList;
import java.util.List;

import static io.trino.spi.StandardErrorCode.IR_ERROR;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.dialect.trino.TrinoDialect.TRINO;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static io.trino.sql.dialect.trino.operationmetadata.LogicalOperationMetadata.LOGICAL_OPERATOR;
import static io.trino.sql.dialect.trino.operationmetadata.LogicalOperationMetadata.NAME;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class Logical
        extends TrinoOperation
{
    private final Result result;
    private final List<Value> terms;
    private final Attributes attributes;

    public Logical(String resultName, List<Value> terms, LogicalOperator logicalOperator, List<Attributes> sourceAttributes)
    {
        this(resultName, terms, logicalOperator, sourceAttributes, Attributes.empty());
    }

    public Logical(String resultName, List<Value> terms, LogicalOperator logicalOperator, List<Attributes> sourceAttributes, Attributes enforcedAttributes)
    {
        super(TRINO, NAME);
        requireNonNull(resultName, "resultName is null");
        requireNonNull(terms, "terms is null");
        requireNonNull(logicalOperator, "logicalOperator is null");
        requireNonNull(sourceAttributes, "sourceAttributes is null");
        requireNonNull(enforcedAttributes, "enforcedAttributes is null");

        this.result = new Result(resultName, irType(BOOLEAN));

        if (terms.size() < 2) {
            throw new TrinoException(IR_ERROR, "logical operation must have at least 2 terms. actual: " + terms.size());
        }
        terms.stream()
                .forEach(term -> {
                    if (!trinoType(term.type()).equals(BOOLEAN)) {
                        throw new TrinoException(IR_ERROR, "all terms of a logical operation must be of boolean type. found: " + trinoType(term.type()).getDisplayName());
                    }
                });
        this.terms = ImmutableList.copyOf(terms);

        if (sourceAttributes.size() != terms.size()) {
            throw new TrinoException(IR_ERROR, format("the number of source attribute maps: %s does not match the number of arguments: %s", sourceAttributes.size(), terms.size()));
        }

        Attributes operationAttributes = LOGICAL_OPERATOR.asAttributes(logicalOperator);

        Attributes.Builder attributes = Attributes.builder();
        attributes.putAll(operationAttributes);
        attributes.putAll(LogicalOperationMetadata.deriveAttributes(operationAttributes, sourceAttributes));

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
        return terms;
    }

    @Override
    public List<Region> regions()
    {
        return ImmutableList.of();
    }

    @Override
    public Attributes attributes()
    {
        return attributes;
    }

    @Override
    public String prettyPrint(int indentLevel, PrintOptions printOptions)
    {
        return "logical :)";
    }

    @Override
    public Operation withArgument(Value newArgument, int index)
    {
        validateArgument(newArgument, index);
        List<Value> newTerms = new ArrayList<>(terms);
        newTerms.set(index, newArgument);
        return new Logical(
                result.name(),
                newTerms,
                LOGICAL_OPERATOR.getAttribute(attributes),
                emptySourceAttributes(terms.size()));
    }

    @Override
    public Operation withResultName(String newName)
    {
        return new Logical(newName, terms, LOGICAL_OPERATOR.getAttribute(attributes), emptySourceAttributes(terms.size()));
    }

    @Override
    public Attributes operationAttributes()
    {
        return filterAttributes(LogicalOperationMetadata.OPERATION_ATTRIBUTES);
    }

    @Override
    public <R, C> R accept(TrinoOperationVisitor<R, C> visitor, C context)
    {
        return visitor.visitLogical(this, context);
    }
}
