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
import io.trino.sql.dialect.trino.operation.Comparison;
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
import static io.trino.sql.dialect.trino.operation.TrinoOperation.emptySourceAttributes;
import static io.trino.sql.dialect.trino.operationmetadata.AttributeDerivationUtils.defaultDeriveIrLevelAttributes;
import static io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.internalEnumAttributeMetadata;
import static java.util.stream.Collectors.partitioningBy;

public class ComparisonOperationMetadata
        implements TrinoOperationMetadata
{
    public static final String NAME = "comparison";

    private static final TrinoAttributeMetadata<ComparisonOperator> COMPARISON_OPERATOR_ATTRIBUTE_METADATA = internalEnumAttributeMetadata(NAME, "operator", ComparisonOperator.class);

    public static final TrinoAttributeSignature<ComparisonOperator> COMPARISON_OPERATOR = COMPARISON_OPERATOR_ATTRIBUTE_METADATA.trinoAttributeSignature();

    public static final Set<TrinoAttributeSignature<?>> OPERATION_ATTRIBUTES = ImmutableSet.of(COMPARISON_OPERATOR);

    @Override
    public String name()
    {
        return NAME;
    }

    @Override
    public Set<TrinoAttributeMetadata<?>> operationAttributes()
    {
        return ImmutableSet.of(COMPARISON_OPERATOR_ATTRIBUTE_METADATA);
    }

    @Override
    public Operation createOperation(String resultName, List<Value> arguments, List<Region> regions, Map<AttributeKey, Object> attributes)
    {
        checkArgument(arguments.size() == 2, "Comparison operation must have exactly two arguments");
        checkArgument(regions.isEmpty(), "Comparison operation does not have regions");

        Map<Boolean, List<Map.Entry<AttributeKey, Object>>> partitionedAttributes = attributes.entrySet().stream()
                .collect(partitioningBy(entry -> inherentOperationAttributeKeys().contains(entry.getKey())));
        Map<AttributeKey, Object> operationAttributes = ImmutableMap.copyOf(partitionedAttributes.get(true));
        Map<AttributeKey, Object> derivedAttributes = ImmutableMap.copyOf(partitionedAttributes.get(false));

        return new Comparison(
                resultName,
                arguments.get(0),
                arguments.get(1),
                COMPARISON_OPERATOR.getAttribute(operationAttributes),
                emptySourceAttributes(arguments.size()),
                derivedAttributes);
    }

    @Override
    public BiFunction<Map<AttributeKey, Object>, List<Map<AttributeKey, Object>>, Map<AttributeKey, Object>> attributeDerivation()
    {
        return ComparisonOperationMetadata::deriveAttributes;
    }

    public static Map<AttributeKey, Object> deriveAttributes(Map<AttributeKey, Object> currentAttributes, List<Map<AttributeKey, Object>> childAttributes)
    {
        checkArgument(childAttributes.size() == 2, "Comparison operation must have exactly two child attributes maps");

        return defaultDeriveIrLevelAttributes(childAttributes);
    }

    public enum ComparisonOperator
    {
        EQUAL("="),
        NOT_EQUAL("<>"),
        LESS_THAN("<"),
        LESS_THAN_OR_EQUAL("<="),
        GREATER_THAN(">"),
        GREATER_THAN_OR_EQUAL(">="),
        IDENTICAL("≡"); // not distinct

        private final String value;

        ComparisonOperator(String value)
        {
            this.value = value;
        }

        public String getValue()
        {
            return value;
        }

        public static ComparisonOperator of(io.trino.sql.ir.Comparison.Operator operator)
        {
            return switch (operator) {
                case EQUAL -> EQUAL;
                case NOT_EQUAL -> NOT_EQUAL;
                case LESS_THAN -> LESS_THAN;
                case LESS_THAN_OR_EQUAL -> LESS_THAN_OR_EQUAL;
                case GREATER_THAN -> GREATER_THAN;
                case GREATER_THAN_OR_EQUAL -> GREATER_THAN_OR_EQUAL;
                case IDENTICAL -> IDENTICAL;
            };
        }
    }
}
