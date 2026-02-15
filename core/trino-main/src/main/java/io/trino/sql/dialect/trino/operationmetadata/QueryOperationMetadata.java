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
import io.trino.sql.dialect.trino.operation.Query;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Operation.AttributeKey;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.sql.dialect.ir.IrDialect.IR;
import static io.trino.sql.dialect.ir.IrDialect.TERMINAL;
import static io.trino.sql.dialect.trino.operationmetadata.AttributeDerivationUtils.passIrLevelAttributes;
import static java.util.stream.Collectors.partitioningBy;

public class QueryOperationMetadata
        implements TrinoOperationMetadata
{
    public static final String NAME = "query";

    @Override
    public String name()
    {
        return NAME;
    }

    @Override
    public Set<TrinoAttributeMetadata<?>> operationAttributes()
    {
        // note: Query operation has the ir.terminal attribute, but it is not operation-specific.
        return ImmutableSet.of();
    }

    @Override
    public Set<AttributeKey> inherentOperationAttributeKeys()
    {
        return ImmutableSet.<AttributeKey>builder()
                .addAll(TrinoOperationMetadata.super.inherentOperationAttributeKeys())
                .add(new AttributeKey(IR, TERMINAL))
                .build();
    }

    @Override
    public Operation createOperation(String resultName, List<Value> arguments, List<Region> regions, Map<AttributeKey, Object> attributes)
    {
        checkArgument(arguments.isEmpty(), "Query operation does not have arguments");
        checkArgument(regions.size() == 1, "Query operation must have exactly one region: the query");

        Map<Boolean, List<Map.Entry<AttributeKey, Object>>> partitionedAttributes = attributes.entrySet().stream()
                .collect(partitioningBy(entry -> inherentOperationAttributeKeys().contains(entry.getKey())));
        Map<AttributeKey, Object> derivedAttributes = ImmutableMap.copyOf(partitionedAttributes.get(false));

        return new Query(
                resultName,
                getOnlyElement(regions).getOnlyBlock().withLabel("^query"),
                derivedAttributes);
    }

    @Override
    public BiFunction<Map<AttributeKey, Object>, List<Map<AttributeKey, Object>>, Map<AttributeKey, Object>> attributeDerivation()
    {
        return QueryOperationMetadata::deriveAttributes;
    }

    public static Map<AttributeKey, Object> deriveAttributes(Map<AttributeKey, Object> currentAttributes, List<Map<AttributeKey, Object>> childAttributes)
    {
        checkArgument(childAttributes.size() == 1, "Query operation must have exactly one child attributes map");

        return passIrLevelAttributes(getOnlyElement(childAttributes));
    }
}
