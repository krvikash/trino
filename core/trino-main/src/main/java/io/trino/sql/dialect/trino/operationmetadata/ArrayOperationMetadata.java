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
import io.trino.sql.newir.Operation.AttributeKey;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

import static io.trino.sql.dialect.ir.IrAttributeUtils.deterministic;
import static io.trino.sql.dialect.ir.IrAttributeUtils.hasNoSideEffects;
import static io.trino.sql.dialect.ir.IrAttributeUtils.hasSideEffects;
import static io.trino.sql.dialect.ir.IrAttributeUtils.nonDeterministic;
import static io.trino.sql.dialect.ir.IrAttributeUtils.nonIdempotent;
import static io.trino.sql.dialect.ir.IrAttributeUtils.unsafe;

public class ArrayOperationMetadata
        implements TrinoOperationMetadata
{
    public static final String NAME = "array";

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
    public BiFunction<Map<AttributeKey, Object>, List<Map<AttributeKey, Object>>, Map<AttributeKey, Object>> attributeDerivation()
    {
        return ArrayOperationMetadata::deriveAttributes;
    }

    public static Map<AttributeKey, Object> deriveAttributes(Map<AttributeKey, Object> currentAttributes, List<Map<AttributeKey, Object>> childAttributes)
    {
        ImmutableMap.Builder<AttributeKey, Object> derivedAttributes = ImmutableMap.builder();

        if (childAttributes.stream().anyMatch(IrAttributeUtils::isUnknownRepeatability)) {
            if (childAttributes.stream().anyMatch(IrAttributeUtils::isKnownNonDeterministic)) {
                nonDeterministic(derivedAttributes);
            }
        }
        else if (childAttributes.stream().anyMatch(IrAttributeUtils::isKnownNonDeterministic)) {
            nonDeterministic(derivedAttributes);
        }
        else if (childAttributes.stream().anyMatch(IrAttributeUtils::isKnownNonIdempotent)) {
            nonIdempotent(derivedAttributes);
        }
        else {
            deterministic(derivedAttributes);
        }

        if (childAttributes.stream().anyMatch(IrAttributeUtils::isKnownUnsafe)) {
            unsafe(derivedAttributes);
        }
        // otherwise safety is unknown because we don't know the safety of the array constructor itself
        // it might fail if too many elements are passed

        if (childAttributes.stream().anyMatch(IrAttributeUtils::isKnownHasSideEffects)) {
            hasSideEffects(derivedAttributes);
        }
        else if (childAttributes.stream().allMatch(IrAttributeUtils::isKnownHasNoSideEffects)) {
            hasNoSideEffects(derivedAttributes);
        }

        return derivedAttributes.buildOrThrow();
    }
}
