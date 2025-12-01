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
import io.trino.spi.type.Type;
import io.trino.sql.dialect.trino.operationmetadata.BindOperationMetadata;
import io.trino.sql.newir.FormatOptions.PrintOptions;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;
import io.trino.type.FunctionType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.trino.spi.StandardErrorCode.IR_ERROR;
import static io.trino.sql.dialect.trino.TrinoDialect.TRINO;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static io.trino.sql.dialect.trino.operationmetadata.BindOperationMetadata.NAME;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class Bind
        extends TrinoOperation
{
    private final Result result;
    private final List<Value> values;
    private final Value lambda;
    private final Map<AttributeKey, Object> attributes;

    public Bind(String resultName, List<Value> values, Value lambda, List<Map<AttributeKey, Object>> sourceAttributes)
    {
        this(resultName, values, lambda, sourceAttributes, ImmutableMap.of());
    }

    public Bind(String resultName, List<Value> values, Value lambda, List<Map<AttributeKey, Object>> sourceAttributes, Map<AttributeKey, Object> enforcedAttributes)
    {
        super(TRINO, NAME);
        requireNonNull(resultName, "resultName is null");
        requireNonNull(values, "values is null");
        requireNonNull(lambda, "lambda is null");
        requireNonNull(sourceAttributes, "sourceAttributes is null");
        requireNonNull(enforcedAttributes, "enforcedAttributes is null");

        List<Type> lambdaArgumentTypes = ((FunctionType) trinoType(lambda.type())).getArgumentTypes();
        if (values.size() > lambdaArgumentTypes.size()) {
            throw new TrinoException(IR_ERROR, format("the number of bind arguments: %s exceeds the number of lambda arguments: %s", values.size(), lambdaArgumentTypes.size()));
        }
        for (int i = 0; i < values.size(); i++) {
            if (!trinoType(values.get(i).type()).equals(lambdaArgumentTypes.get(i))) {
                throw new TrinoException(IR_ERROR, format("bind argument %s has mismatching type: %s. expected: %s", i, trinoType(values.get(i).type()), lambdaArgumentTypes.get(i)));
            }
        }
        Type resultType = new FunctionType(lambdaArgumentTypes.subList(values.size(), lambdaArgumentTypes.size()), ((FunctionType) trinoType(lambda.type())).getReturnType());

        this.result = new Result(resultName, irType(resultType));

        this.values = ImmutableList.copyOf(values);

        this.lambda = lambda;

        // validate source attributes count -- one entry per each argument: values-list, and lambda
        if (sourceAttributes.size() != values.size() + 1) {
            throw new TrinoException(IR_ERROR, format("the number of source attribute maps: %s does not match the number of arguments: %s", sourceAttributes.size(), values.size() + 1));
        }

        ImmutableMap.Builder<AttributeKey, Object> attributes = ImmutableMap.builder();
        attributes.putAll(BindOperationMetadata.deriveAttributes(ImmutableMap.of(), sourceAttributes));
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
        return ImmutableList.<Value>builder()
                .addAll(values)
                .add(lambda)
                .build();
    }

    @Override
    public List<Region> regions()
    {
        return ImmutableList.of();
    }

    @Override
    public Map<AttributeKey, Object> attributes()
    {
        return attributes;
    }

    @Override
    public String prettyPrint(int indentLevel, PrintOptions printOptions)
    {
        return "bind :)";
    }

    @Override
    public Operation withArgument(Value newArgument, int index)
    {
        validateArgument(newArgument, index);
        List<Value> newValues = new ArrayList<>(values);
        if (index < values.size()) {
            newValues.set(index, newArgument);
        }
        return new Bind(
                result.name(),
                newValues,
                index == values.size() ? newArgument : lambda,
                emptySourceAttributes(values.size() + 1));
    }

    @Override
    public Operation withResultName(String newName)
    {
        return new Bind(newName, values, lambda, emptySourceAttributes(values.size() + 1));
    }

    @Override
    public Map<AttributeKey, Object> operationAttributes()
    {
        return ImmutableMap.of();
    }

    @Override
    public <R, C> R accept(TrinoOperationVisitor<R, C> visitor, C context)
    {
        return visitor.visitBind(this, context);
    }
}
