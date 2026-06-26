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
import io.trino.sql.dialect.trino.operation.Case;
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

public class CaseOperationMetadata
        implements TrinoOperationMetadata
{
    public static final String NAME = "case";

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
        checkArgument(arguments.size() >= 3, "Case operation must have at least three arguments");
        checkArgument(arguments.size() % 2 == 1, "Case operation must have odd number of arguments");
        checkArgument(regions.isEmpty(), "Case operation does not have regions");

        Attributes.Partition partitionedAttributes = attributes.partitionKeys(inherentOperationAttributeKeys()::contains);
        Attributes derivedAttributes = partitionedAttributes.nonMatching();

        return new Case(
                resultName,
                arguments.subList(0, arguments.size() / 2),
                arguments.subList(arguments.size() / 2, arguments.size() - 1),
                arguments.getLast(),
                emptySourceAttributes(arguments.size()),
                derivedAttributes);
    }

    @Override
    public BiFunction<Attributes, List<Attributes>, Attributes> attributeDerivation()
    {
        return CaseOperationMetadata::deriveAttributes;
    }

    public static Attributes deriveAttributes(Attributes currentAttributes, List<Attributes> childAttributes)
    {
        checkArgument(childAttributes.size() >= 3, "Case operation must have at least three child attributes maps");
        checkArgument(childAttributes.size() % 2 == 1, "Case operation must have odd number of child attributes maps");

        return defaultDeriveIrLevelAttributes(childAttributes);
    }
}
