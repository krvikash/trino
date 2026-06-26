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
import io.trino.sql.dialect.ir.IrAttributeUtils;
import io.trino.sql.dialect.trino.operation.AssignUniqueId;
import io.trino.sql.newir.Attributes;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;

import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.sql.dialect.ir.IrAttributeUtils.hasNoSideEffects;
import static io.trino.sql.dialect.ir.IrAttributeUtils.hasSideEffects;
import static io.trino.sql.dialect.ir.IrAttributeUtils.nonDeterministic;
import static io.trino.sql.dialect.ir.IrAttributeUtils.safe;

public class AssignUniqueIdOperationMetadata
        implements TrinoOperationMetadata
{
    public static final String NAME = "assign_unique_id";

    @Override
    public String name()
    {
        return NAME;
    }

    @Override
    public Set<TrinoAttributeMetadata<?>> operationAttributes()
    {
        return ImmutableSet.of();
    }

    @Override
    public Operation createOperation(String resultName, List<Value> arguments, List<Region> regions, Attributes attributes)
    {
        checkArgument(arguments.size() == 1, "AssignUniqueId operation must have exactly one argument: the input relation");
        checkArgument(regions.isEmpty(), "AssignUniqueId operation does not have regions");

        Attributes.Partition partitionedAttributes = attributes.partitionKeys(inherentOperationAttributeKeys()::contains);
        Attributes derivedAttributes = partitionedAttributes.nonMatching();

        return new AssignUniqueId(
                resultName,
                getOnlyElement(arguments),
                Attributes.empty(),
                derivedAttributes);
    }

    @Override
    public BiFunction<Attributes, List<Attributes>, Attributes> attributeDerivation()
    {
        return AssignUniqueIdOperationMetadata::deriveAttributes;
    }

    public static Attributes deriveAttributes(Attributes currentAttributes, List<Attributes> childAttributes)
    {
        checkArgument(childAttributes.size() == 1, "AssignUniqueId operation must have exactly one child attributes map: for the input");

        Attributes.Builder derivedAttributes = Attributes.builder();
        nonDeterministic(derivedAttributes);

        if (childAttributes.stream().allMatch(IrAttributeUtils::isKnownSafe)) {
            safe(derivedAttributes);
        }

        if (childAttributes.stream().anyMatch(IrAttributeUtils::isKnownHasSideEffects)) {
            hasSideEffects(derivedAttributes);
        }
        else if (childAttributes.stream().allMatch(IrAttributeUtils::isKnownHasNoSideEffects)) {
            hasNoSideEffects(derivedAttributes);
        }

        return derivedAttributes.buildOrThrow();
    }
}
