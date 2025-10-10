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

import java.util.Set;

import static io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.internalEnumAttributeMetadata;

public class CorrelatedJoinOperationMetadata
        implements TrinoOperationMetadata
{
    public static final String NAME = "correlated_join";

    private static final TrinoAttributeMetadata<JoinType> JOIN_TYPE_ATTRIBUTE_METADATA = internalEnumAttributeMetadata(NAME, "type", JoinType.class);

    public static final TrinoAttributeSignature<JoinType> JOIN_TYPE = JOIN_TYPE_ATTRIBUTE_METADATA.trinoAttributeSignature();

    @Override
    public String name()
    {
        return NAME;
    }

    @Override
    public Set<TrinoAttributeMetadata<?>> operationAttributes()
    {
        return ImmutableSet.of(JOIN_TYPE_ATTRIBUTE_METADATA);
    }

    public enum JoinType
    {
        INNER,
        LEFT,
        RIGHT,
        FULL;

        public static JoinType of(io.trino.sql.planner.plan.JoinType joinType)
        {
            return switch (joinType) {
                case INNER -> INNER;
                case LEFT -> LEFT;
                case RIGHT -> RIGHT;
                case FULL -> FULL;
            };
        }
    }
}
