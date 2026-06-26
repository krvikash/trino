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
package io.trino.sql.dialect.trino.operationmetadata;

import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;
import io.trino.sql.dialect.trino.operation.Constant;
import io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.ConstantValue;
import io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.TrinoAttributeSignature;
import io.trino.sql.newir.Attributes;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;
import org.assertj.core.util.VisibleForTesting;

import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.sql.dialect.ir.IrAttributeUtils.deterministic;
import static io.trino.sql.dialect.ir.IrAttributeUtils.hasNoSideEffects;
import static io.trino.sql.dialect.ir.IrAttributeUtils.safe;
import static io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.prefixedName;
import static java.util.Objects.requireNonNull;

public class ConstantOperationMetadata
        implements TrinoOperationMetadata
{
    public static final String NAME = "constant";

    public static final TrinoAttributeSignature<ConstantValue> CONSTANT_VALUE = new TrinoAttributeSignature<>(prefixedName(NAME, "value"), false);

    public static final Set<TrinoAttributeSignature<?>> OPERATION_ATTRIBUTES = ImmutableSet.of(CONSTANT_VALUE);

    private final TrinoAttributeMetadata<ConstantValue> constantValueTrinoAttributeMetadata;

    public ConstantOperationMetadata(JsonCodec<ConstantValue> constantValueCodec)
    {
        this(constantValueCodec::fromJson, constantValueCodec::toJson);
    }

    @VisibleForTesting
    public ConstantOperationMetadata(Function<String, ConstantValue> constantValueParseMethod, Function<ConstantValue, String> constantValuePrintMethod)
    {
        requireNonNull(constantValueParseMethod, "constantValueParseMethod is null");
        requireNonNull(constantValuePrintMethod, "constantValuePrintMethod is null");

        this.constantValueTrinoAttributeMetadata = new TrinoAttributeMetadata<>(CONSTANT_VALUE, constantValueParseMethod, constantValuePrintMethod);
    }

    @Override
    public String name()
    {
        return NAME;
    }

    @Override
    public Set<TrinoAttributeMetadata<?>> operationAttributes()
    {
        return ImmutableSet.of(constantValueTrinoAttributeMetadata);
    }

    @Override
    public Operation createOperation(String resultName, List<Value> arguments, List<Region> regions, Attributes attributes)
    {
        checkArgument(arguments.isEmpty(), "Constant operation does not have arguments");
        checkArgument(regions.isEmpty(), "Constant operation does not have regions");

        Attributes.Partition partitionedAttributes = attributes.partitionKeys(inherentOperationAttributeKeys()::contains);
        Attributes operationAttributes = partitionedAttributes.matching();
        Attributes derivedAttributes = partitionedAttributes.nonMatching();

        ConstantValue constantValue = CONSTANT_VALUE.getAttribute(operationAttributes);

        return new Constant(
                resultName,
                constantValue.getType(),
                constantValue.getValue(),
                derivedAttributes);
    }

    @Override
    public BiFunction<Attributes, List<Attributes>, Attributes> attributeDerivation()
    {
        return ConstantOperationMetadata::deriveAttributes;
    }

    public static Attributes deriveAttributes(Attributes currentAttributes, List<Attributes> childAttributes)
    {
        checkArgument(childAttributes.isEmpty(), "Constant operation must have exactly zero child attributes maps");

        Attributes.Builder derivedAttributes = Attributes.builder();
        deterministic(derivedAttributes);
        safe(derivedAttributes);
        hasNoSideEffects(derivedAttributes);

        return derivedAttributes.buildOrThrow();
    }
}
