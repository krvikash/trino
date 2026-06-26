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
import io.trino.sql.dialect.trino.operation.FieldReference;
import io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.TrinoAttributeSignature;
import io.trino.sql.newir.Attributes;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;

import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.sql.dialect.ir.IrAttributeDerivationUtils.defaultDeriveIrLevelAttributes;
import static io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.internalIntegerAttributeMetadata;

public class FieldReferenceOperationMetadata
        implements TrinoOperationMetadata
{
    public static final String NAME = "field_reference";

    private static final TrinoAttributeMetadata<Integer> FIELD_INDEX_ATTRIBUTE_METADATA = internalIntegerAttributeMetadata(NAME, "index");

    public static final TrinoAttributeSignature<Integer> FIELD_INDEX = FIELD_INDEX_ATTRIBUTE_METADATA.trinoAttributeSignature();

    public static final Set<TrinoAttributeSignature<?>> OPERATION_ATTRIBUTES = ImmutableSet.of(FIELD_INDEX);

    @Override
    public String name()
    {
        return NAME;
    }

    @Override
    public Set<TrinoAttributeMetadata<?>> operationAttributes()
    {
        return ImmutableSet.of(FIELD_INDEX_ATTRIBUTE_METADATA);
    }

    @Override
    public Operation createOperation(String resultName, List<Value> arguments, List<Region> regions, Attributes attributes)
    {
        checkArgument(arguments.size() == 1, "FieldReference operation must have exactly one argument: the base row value");
        checkArgument(regions.isEmpty(), "FieldReference operation does not have regions");

        Attributes.Partition partitionedAttributes = attributes.partitionKeys(inherentOperationAttributeKeys()::contains);
        Attributes operationAttributes = partitionedAttributes.matching();
        Attributes derivedAttributes = partitionedAttributes.nonMatching();

        return new FieldReference(
                resultName,
                getOnlyElement(arguments),
                FIELD_INDEX.getAttribute(operationAttributes),
                Attributes.empty(),
                derivedAttributes);
    }

    @Override
    public BiFunction<Attributes, List<Attributes>, Attributes> attributeDerivation()
    {
        return FieldReferenceOperationMetadata::deriveAttributes;
    }

    public static Attributes deriveAttributes(Attributes currentAttributes, List<Attributes> childAttributes)
    {
        checkArgument(childAttributes.size() == 1, "FieldReference operation must have exactly one child attributes map");

        // TODO consider source attributes for the selected field
        return defaultDeriveIrLevelAttributes(childAttributes);
    }
}
