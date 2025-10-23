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
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.type.Type;
import io.trino.sql.dialect.trino.operationmetadata.ConstantOperationMetadata;
import io.trino.sql.newir.FormatOptions.PrintOptions;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;

import java.util.List;
import java.util.Map;

import static io.trino.sql.dialect.trino.TrinoDialect.TRINO;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static io.trino.sql.dialect.trino.operationmetadata.ConstantOperationMetadata.CONSTANT_VALUE;
import static io.trino.sql.dialect.trino.operationmetadata.ConstantOperationMetadata.NAME;
import static java.util.Objects.requireNonNull;

public final class Constant
        extends TrinoOperation
{
    private final Result result;
    private final Map<AttributeKey, Object> attributes;

    public Constant(String resultName, Type type, Object value)
    {
        super(TRINO, NAME);
        requireNonNull(resultName, "resultName is null");

        this.result = new Result(resultName, irType(type));

        this.attributes = CONSTANT_VALUE.asMap(new NullableValue(type, value));
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
        return "pretty constant";
    }

    @Override
    public Operation withResultName(String newName)
    {
        NullableValue constantValue = CONSTANT_VALUE.getAttribute(attributes);
        return new Constant(newName, constantValue.getType(), constantValue.getValue());
    }

    @Override
    public Map<AttributeKey, Object> operationAttributes()
    {
        return filterAttributes(ConstantOperationMetadata.OPERATION_ATTRIBUTES);
    }

    @Override
    public <R, C> R accept(TrinoOperationVisitor<R, C> visitor, C context)
    {
        return visitor.visitConstant(this, context);
    }
}
