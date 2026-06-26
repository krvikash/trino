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
import io.trino.spi.type.Type;
import io.trino.sql.dialect.trino.operation.Cast;
import io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.TrinoAttributeSignature;
import io.trino.sql.newir.Attributes;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;

import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.sql.dialect.ir.IrAttributeUtils.deterministic;
import static io.trino.sql.dialect.ir.IrAttributeUtils.hasNoSideEffects;
import static io.trino.sql.dialect.ir.IrAttributeUtils.hasSideEffects;
import static io.trino.sql.dialect.ir.IrAttributeUtils.isKnownDeterministic;
import static io.trino.sql.dialect.ir.IrAttributeUtils.isKnownHasNoSideEffects;
import static io.trino.sql.dialect.ir.IrAttributeUtils.isKnownHasSideEffects;
import static io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.prefixedName;
import static java.util.Objects.requireNonNull;

public class CastOperationMetadata
        implements TrinoOperationMetadata
{
    public static final String NAME = "cast";

    public static final TrinoAttributeSignature<Type> TO_TYPE = new TrinoAttributeSignature<>(prefixedName(NAME, "to_type"), false);

    public static final Set<TrinoAttributeSignature<?>> OPERATION_ATTRIBUTES = ImmutableSet.of(TO_TYPE);

    private final TrinoAttributeMetadata<Type> toTypeTrinoAttributeMetadata;

    public CastOperationMetadata(Function<String, Type> typeDeserializer)
    {
        requireNonNull(typeDeserializer, "typeDeserializer is null");

        this.toTypeTrinoAttributeMetadata = new TrinoAttributeMetadata<>(TO_TYPE, typeDeserializer, type -> type.getTypeId().getId());
    }

    @Override
    public String name()
    {
        return NAME;
    }

    @Override
    public Set<TrinoAttributeMetadata<?>> operationAttributes()
    {
        return ImmutableSet.of(toTypeTrinoAttributeMetadata);
    }

    @Override
    public Operation createOperation(String resultName, List<Value> arguments, List<Region> regions, Attributes attributes)
    {
        checkArgument(arguments.size() == 1, "Cast operation must have exactly one argument: the input value");
        checkArgument(regions.isEmpty(), "Cast operation does not have regions");

        Attributes.Partition partitionedAttributes = attributes.partitionKeys(inherentOperationAttributeKeys()::contains);
        Attributes operationAttributes = partitionedAttributes.matching();
        Attributes derivedAttributes = partitionedAttributes.nonMatching();

        return new Cast(
                resultName,
                getOnlyElement(arguments),
                TO_TYPE.getAttribute(operationAttributes),
                Attributes.empty(),
                derivedAttributes);
    }

    @Override
    public BiFunction<Attributes, List<Attributes>, Attributes> attributeDerivation()
    {
        return CastOperationMetadata::deriveAttributes;
    }

    public static Attributes deriveAttributes(Attributes currentAttributes, List<Attributes> childAttributes)
    {
        checkArgument(childAttributes.size() == 1, "Cast operation must have exactly one child attributes map");

        Attributes inputAttributes = getOnlyElement(childAttributes);

        Attributes.Builder derivedAttributes = Attributes.builder();

        // IR-level attributes
        if (isKnownDeterministic(inputAttributes)) {
            deterministic(derivedAttributes);
        }

        // if the input is known safe, the cast safety depends on the cast itself
        // TODO determine cast safety

        if (isKnownHasSideEffects(inputAttributes)) {
            hasSideEffects(derivedAttributes);
        }
        else if (isKnownHasNoSideEffects(inputAttributes)) {
            hasNoSideEffects(derivedAttributes);
        }

        return derivedAttributes.buildOrThrow();
    }
}
