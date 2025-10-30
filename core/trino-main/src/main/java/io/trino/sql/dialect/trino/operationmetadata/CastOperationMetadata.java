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
import io.trino.sql.newir.Operation.AttributeKey;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.sql.dialect.ir.IrAttributeUtils.deterministic;
import static io.trino.sql.dialect.ir.IrAttributeUtils.hasNoSideEffects;
import static io.trino.sql.dialect.ir.IrAttributeUtils.hasSideEffects;
import static io.trino.sql.dialect.ir.IrAttributeUtils.isKnownDeterministic;
import static io.trino.sql.dialect.ir.IrAttributeUtils.isKnownHasNoSideEffects;
import static io.trino.sql.dialect.ir.IrAttributeUtils.isKnownHasSideEffects;
import static io.trino.sql.dialect.ir.IrAttributeUtils.isKnownUnsafe;
import static io.trino.sql.dialect.ir.IrAttributeUtils.unsafe;

public class CastOperationMetadata
        implements TrinoOperationMetadata
{
    public static final String NAME = "cast";

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
        return CastOperationMetadata::deriveAttributes;
    }

    public static Map<AttributeKey, Object> deriveAttributes(Map<AttributeKey, Object> currentAttributes, List<Map<AttributeKey, Object>> childAttributes)
    {
        Map<AttributeKey, Object> inputAttributes = getOnlyElement(childAttributes);

        ImmutableMap.Builder<AttributeKey, Object> derivedAttributes = ImmutableMap.builder();

        // IR-level attributes
        if (isKnownDeterministic(inputAttributes)) {
            deterministic(derivedAttributes);
        }

        if (isKnownUnsafe(inputAttributes)) {
            unsafe(derivedAttributes);
        }
        // otherwise safety is unknown because we don't know the safety of the function itself
        // TODO determine cast safety

        if (isKnownHasSideEffects(inputAttributes)) {
            hasSideEffects(derivedAttributes);
        }
        else if (isKnownHasNoSideEffects(inputAttributes)) {
            hasNoSideEffects(derivedAttributes);
        }

        return derivedAttributes.buildOrThrow();
    }
}
