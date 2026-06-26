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
import io.trino.sql.dialect.trino.operation.Filter;
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

public class FilterOperationMetadata
        implements TrinoOperationMetadata
{
    public static final String NAME = "filter";

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
        checkArgument(arguments.size() == 1, "Filter operation must have exactly one argument: the input relation");
        checkArgument(regions.size() == 1, "Filter operation must have exactly one region: the predicate");

        Attributes.Partition partitionedAttributes = attributes.partitionKeys(inherentOperationAttributeKeys()::contains);
        Attributes derivedAttributes = partitionedAttributes.nonMatching();

        return new Filter(
                resultName,
                getOnlyElement(arguments),
                getOnlyElement(regions).getOnlyBlock().withLabel("^predicate"),
                Attributes.empty(),
                derivedAttributes);
    }

    @Override
    public BiFunction<Attributes, List<Attributes>, Attributes> attributeDerivation()
    {
        return FilterOperationMetadata::deriveAttributes;
    }

    public static Attributes deriveAttributes(Attributes currentAttributes, List<Attributes> childAttributes)
    {
        checkArgument(childAttributes.size() == 2, "Filter operation must have exactly two child attributes maps: one for the input, and one for the predicate");

        return defaultDeriveIrLevelAttributes(childAttributes);
    }
}
