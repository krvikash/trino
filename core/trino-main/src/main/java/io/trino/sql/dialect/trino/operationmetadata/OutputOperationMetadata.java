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
import io.trino.sql.dialect.ir.IrAttributeUtils;
import io.trino.sql.dialect.trino.operation.Output;
import io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.TrinoAttributeSignature;
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
import static io.trino.sql.dialect.ir.IrAttributeUtils.deterministic;
import static io.trino.sql.dialect.ir.IrAttributeUtils.hasSideEffects;
import static io.trino.sql.dialect.ir.IrAttributeUtils.safe;
import static io.trino.sql.dialect.ir.IrDialect.IR;
import static io.trino.sql.dialect.ir.IrDialect.TERMINAL;
import static io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.internalStringListAttributeMetadata;
import static java.util.stream.Collectors.partitioningBy;

public class OutputOperationMetadata
        implements TrinoOperationMetadata
{
    public static final String NAME = "output";

    private static final TrinoAttributeMetadata<List<String>> COLUMN_NAMES_ATTRIBUTE_METADATA = internalStringListAttributeMetadata(NAME, "column_names");

    public static final TrinoAttributeSignature<List<String>> COLUMN_NAMES = COLUMN_NAMES_ATTRIBUTE_METADATA.trinoAttributeSignature();

    public static final Set<TrinoAttributeSignature<?>> OPERATION_ATTRIBUTES = ImmutableSet.of(COLUMN_NAMES);

    @Override
    public String name()
    {
        return NAME;
    }

    @Override
    public Set<TrinoAttributeMetadata<?>> operationAttributes()
    {
        // note: Output operation also has the ir.terminal attribute, but it is not operation-specific.
        return ImmutableSet.of(COLUMN_NAMES_ATTRIBUTE_METADATA);
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
        checkArgument(arguments.size() == 1, "Output operation must have exactly one argument: the input relation");
        checkArgument(regions.size() == 1, "Output operation must have exactly one region: the field selector");

        Map<Boolean, List<Map.Entry<AttributeKey, Object>>> partitionedAttributes = attributes.entrySet().stream()
                .collect(partitioningBy(entry -> inherentOperationAttributeKeys().contains(entry.getKey())));
        Map<AttributeKey, Object> operationAttributes = ImmutableMap.copyOf(partitionedAttributes.get(true));
        Map<AttributeKey, Object> derivedAttributes = ImmutableMap.copyOf(partitionedAttributes.get(false));

        return new Output(
                resultName,
                getOnlyElement(arguments),
                getOnlyElement(regions).getOnlyBlock().withLabel("^outputFieldSelector"),
                COLUMN_NAMES.getAttribute(operationAttributes),
                ImmutableMap.of(),
                derivedAttributes);
    }

    @Override
    public BiFunction<Map<AttributeKey, Object>, List<Map<AttributeKey, Object>>, Map<AttributeKey, Object>> attributeDerivation()
    {
        return OutputOperationMetadata::deriveAttributes;
    }

    public static Map<AttributeKey, Object> deriveAttributes(Map<AttributeKey, Object> currentAttributes, List<Map<AttributeKey, Object>> childAttributes)
    {
        checkArgument(childAttributes.size() == 2, "Output operation must have exactly two child attributes maps: one for the input, and one for the field selector");

        ImmutableMap.Builder<AttributeKey, Object> derivedAttributes = ImmutableMap.builder();

        if (childAttributes.stream().allMatch(IrAttributeUtils::isKnownDeterministic)) {
            deterministic(derivedAttributes);
        }

        if (childAttributes.stream().allMatch(IrAttributeUtils::isKnownSafe)) {
            safe(derivedAttributes);
        }

        // Output operation has side effects in that it writes data out
        hasSideEffects(derivedAttributes);

        return derivedAttributes.buildOrThrow();
    }
}
