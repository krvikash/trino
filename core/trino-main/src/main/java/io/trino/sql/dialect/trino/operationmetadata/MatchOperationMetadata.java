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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.sql.dialect.trino.operation.Match;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Operation.AttributeKey;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.sql.dialect.ir.IrAttributeDerivationUtils.defaultDeriveIrLevelAttributes;
import static io.trino.sql.dialect.trino.operation.TrinoOperation.emptySourceAttributes;
import static java.util.stream.Collectors.partitioningBy;

public class MatchOperationMetadata
        implements TrinoOperationMetadata
{
    public static final String NAME = "switch";

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
    public Operation createOperation(String resultName, List<Value> arguments, List<Region> regions, Map<AttributeKey, Object> attributes)
    {
        checkArgument(arguments.size() >= 4, "Switch operation must have at least four arguments");
        checkArgument(arguments.size() % 2 == 0, "Switch operation must have even number of arguments");
        checkArgument(regions.isEmpty(), "Switch operation does not have regions");

        Map<Boolean, List<Map.Entry<AttributeKey, Object>>> partitionedAttributes = attributes.entrySet().stream()
                .collect(partitioningBy(entry -> inherentOperationAttributeKeys().contains(entry.getKey())));
        Map<AttributeKey, Object> derivedAttributes = ImmutableMap.copyOf(partitionedAttributes.get(false));

        return new Match(
                resultName,
                arguments.getFirst(),
                arguments.subList(1, arguments.size() / 2),
                arguments.subList(arguments.size() / 2, arguments.size() - 1),
                arguments.getLast(),
                emptySourceAttributes(arguments.size()),
                derivedAttributes);
    }

    @Override
    public BiFunction<Map<AttributeKey, Object>, List<Map<AttributeKey, Object>>, Map<AttributeKey, Object>> attributeDerivation()
    {
        return MatchOperationMetadata::deriveAttributes;
    }

    public static Map<AttributeKey, Object> deriveAttributes(Map<AttributeKey, Object> currentAttributes, List<Map<AttributeKey, Object>> childAttributes)
    {
        checkArgument(childAttributes.size() >= 4, "Switch operation must have at least four child attributes maps");
        checkArgument(childAttributes.size() % 2 == 0, "Switch operation must have even number of child attributes maps");

        return defaultDeriveIrLevelAttributes(childAttributes);
    }
}
