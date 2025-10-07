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
import io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.TrinoAttributeSignature;
import io.trino.sql.ir.Comparison;

import java.util.Set;

import static io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.internalEnumAttributeMetadata;

public class ComparisonOperationMetadata
        implements TrinoOperationMetadata
{
    public static final String NAME = "comparison";

    private static final TrinoAttributeMetadata<ComparisonOperator> COMPARISON_OPERATOR_ATTRIBUTE_METADATA = internalEnumAttributeMetadata(NAME, "comparison_operator", ComparisonOperator.class);

    public static final TrinoAttributeSignature<ComparisonOperator> COMPARISON_OPERATOR = COMPARISON_OPERATOR_ATTRIBUTE_METADATA.trinoAttributeSignature();

    @Override
    public Set<TrinoAttributeMetadata<?>> operationAttributes()
    {
        return ImmutableSet.of(COMPARISON_OPERATOR_ATTRIBUTE_METADATA);
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

        public static ComparisonOperator of(Comparison.Operator operator)
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
