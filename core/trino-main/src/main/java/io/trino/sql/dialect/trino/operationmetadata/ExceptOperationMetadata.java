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
import io.trino.sql.dialect.trino.operation.Except;
import io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.TrinoAttributeSignature;
import io.trino.sql.newir.Attributes;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;

import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.sql.dialect.ir.IrAttributeDerivationUtils.defaultDeriveIrLevelAttributes;
import static io.trino.sql.dialect.trino.operation.TrinoOperation.emptySourceAttributes;
import static io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.internalBooleanAttributeMetadata;

public class ExceptOperationMetadata
        implements TrinoOperationMetadata
{
    public static final String NAME = "except";

    private static final TrinoAttributeMetadata<Boolean> DISTINCT_ATTRIBUTE_METADATA = internalBooleanAttributeMetadata(NAME, "distinct");

    public static final TrinoAttributeSignature<Boolean> DISTINCT = DISTINCT_ATTRIBUTE_METADATA.trinoAttributeSignature();

    public static final Set<TrinoAttributeSignature<?>> OPERATION_ATTRIBUTES = ImmutableSet.of(DISTINCT);

    @Override
    public String name()
    {
        return NAME;
    }

    @Override
    public Set<TrinoAttributeMetadata<?>> operationAttributes()
    {
        return ImmutableSet.of(DISTINCT_ATTRIBUTE_METADATA);
    }

    @Override
    public Operation createOperation(String resultName, List<Value> arguments, List<Region> regions, Attributes attributes)
    {
        checkArgument(!arguments.isEmpty(), "Except operation must have at least one argument: an input relation");
        checkArgument(arguments.size() == regions.size(), "Except operation must have one region per input relation");

        Attributes.Partition partitionedAttributes = attributes.partitionKeys(inherentOperationAttributeKeys()::contains);
        Attributes operationAttributes = partitionedAttributes.matching();
        Attributes derivedAttributes = partitionedAttributes.nonMatching();

        return new Except(
                resultName,
                arguments,
                regions.stream()
                        .map(Region::getOnlyBlock)
                        .map(block -> block.withLabel("^inputSelector"))
                        .toList(),
                DISTINCT.getAttribute(operationAttributes),
                emptySourceAttributes(arguments.size()),
                derivedAttributes);
    }

    @Override
    public BiFunction<Attributes, List<Attributes>, Attributes> attributeDerivation()
    {
        return ExceptOperationMetadata::deriveAttributes;
    }

    public static Attributes deriveAttributes(Attributes currentAttributes, List<Attributes> childAttributes)
    {
        return defaultDeriveIrLevelAttributes(childAttributes);
    }
}
