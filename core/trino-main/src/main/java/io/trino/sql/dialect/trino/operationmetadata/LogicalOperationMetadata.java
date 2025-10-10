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
import io.trino.sql.ir.Logical;

import java.util.Set;

import static io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.internalEnumAttributeMetadata;

public class LogicalOperationMetadata
        implements TrinoOperationMetadata
{
    public static final String NAME = "logical";

    private static final TrinoAttributeMetadata<LogicalOperator> LOGICAL_OPERATOR_ATTRIBUTE_METADATA = internalEnumAttributeMetadata(NAME, "operator", LogicalOperator.class);

    public static final TrinoAttributeSignature<LogicalOperator> LOGICAL_OPERATOR = LOGICAL_OPERATOR_ATTRIBUTE_METADATA.trinoAttributeSignature();

    @Override
    public String name()
    {
        return NAME;
    }

    @Override
    public Set<TrinoAttributeMetadata<?>> operationAttributes()
    {
        return ImmutableSet.of(LOGICAL_OPERATOR_ATTRIBUTE_METADATA);
    }

    public enum LogicalOperator
    {
        AND,
        OR;

        public static LogicalOperator of(Logical.Operator operator)
        {
            return switch (operator) {
                case AND -> AND;
                case OR -> OR;
            };
        }
    }
}
