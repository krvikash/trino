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
package io.trino.sql.dialect.ir;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import io.trino.metadata.ResolvedFunction;
import io.trino.sql.newir.Operation.AttributeKey;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.trino.sql.dialect.ir.IrAttributeUtils.deterministic;
import static io.trino.sql.dialect.ir.IrAttributeUtils.hasNoSideEffects;
import static io.trino.sql.dialect.ir.IrAttributeUtils.hasSideEffects;
import static io.trino.sql.dialect.ir.IrAttributeUtils.nonDeterministic;
import static io.trino.sql.dialect.ir.IrAttributeUtils.nonIdempotent;
import static io.trino.sql.dialect.ir.IrAttributeUtils.safe;
import static io.trino.sql.dialect.ir.IrDialect.HAS_SIDE_EFFECTS;
import static io.trino.sql.dialect.ir.IrDialect.IR;
import static io.trino.sql.dialect.ir.IrDialect.REPEATABILITY;
import static io.trino.sql.dialect.ir.IrDialect.SAFE;

public class IrAttributeDerivationUtils
{
    private IrAttributeDerivationUtils() {}

    public static Map<AttributeKey, Object> defaultDeriveIrLevelAttributes(List<Map<AttributeKey, Object>> childAttributes)
    {
        ImmutableMap.Builder<AttributeKey, Object> derivedAttributes = ImmutableMap.builder();

        if (childAttributes.stream().allMatch(IrAttributeUtils::isKnownDeterministic)) {
            deterministic(derivedAttributes);
        }

        if (childAttributes.stream().allMatch(IrAttributeUtils::isKnownSafe)) {
            safe(derivedAttributes);
        }

        if (childAttributes.stream().anyMatch(IrAttributeUtils::isKnownHasSideEffects)) {
            hasSideEffects(derivedAttributes);
        }
        else if (childAttributes.stream().allMatch(IrAttributeUtils::isKnownHasNoSideEffects)) {
            hasNoSideEffects(derivedAttributes);
        }

        return derivedAttributes.buildOrThrow();
    }

    public static Map<AttributeKey, Object> defaultDeriveFunctionCallIrLevelAttributes(ResolvedFunction resolvedFunction, List<Map<AttributeKey, Object>> childAttributes)
    {
        ImmutableMap.Builder<AttributeKey, Object> derivedAttributes = ImmutableMap.builder();

        if (!resolvedFunction.deterministic()) {
            nonDeterministic(derivedAttributes);
        }
        else if (childAttributes.stream().allMatch(IrAttributeUtils::isKnownDeterministic)) {
            deterministic(derivedAttributes);
        }

        // if any child is unknown safety, the function call is unknown safety
        // if all children are known safe, the function call safety depends on the function itself
        // TODO derive safety based on the ResolvedFunction metadata if all children are known safe

        if (childAttributes.stream().anyMatch(IrAttributeUtils::isKnownHasSideEffects)) {
            hasSideEffects(derivedAttributes);
        }
        else if (childAttributes.stream().allMatch(IrAttributeUtils::isKnownHasNoSideEffects)) {
            hasNoSideEffects(derivedAttributes);
        }
        // we assume that the function itself has no side effects

        return derivedAttributes.buildOrThrow();
    }

    public static Map<AttributeKey, Object> defaultComposeIrLevelAttributes(List<Map<AttributeKey, Object>> childAttributes)
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

        if (childAttributes.stream().allMatch(IrAttributeUtils::isKnownSafe)) {
            safe(derivedAttributes);
        }

        if (childAttributes.stream().anyMatch(IrAttributeUtils::isKnownHasSideEffects)) {
            hasSideEffects(derivedAttributes);
        }
        else if (childAttributes.stream().allMatch(IrAttributeUtils::isKnownHasNoSideEffects)) {
            hasNoSideEffects(derivedAttributes);
        }

        return derivedAttributes.buildOrThrow();
    }

    public static Map<AttributeKey, Object> passIrLevelAttributes(Map<AttributeKey, Object> childAttributes)
    {
        Set<AttributeKey> irLevelAttributeKeys = ImmutableSet.of(
                new AttributeKey(IR, REPEATABILITY),
                new AttributeKey(IR, SAFE),
                new AttributeKey(IR, HAS_SIDE_EFFECTS));

        return Maps.filterKeys(childAttributes, irLevelAttributeKeys::contains);
    }

    public static Map<AttributeKey, Object> defaultDeriveIrLevelAttributesWithPassthroughSource(Map<AttributeKey, Object> passthroughSourceAttributes, List<Map<AttributeKey, Object>> childAttributes)
    {
        ImmutableMap.Builder<AttributeKey, Object> derivedAttributes = ImmutableMap.builder();

        // propagate repeatability from the passthrough source
        derivedAttributes.putAll(getRepeatabilityAttribute(passthroughSourceAttributes));

        if (childAttributes.stream().allMatch(IrAttributeUtils::isKnownSafe)) {
            safe(derivedAttributes);
        }

        if (childAttributes.stream().anyMatch(IrAttributeUtils::isKnownHasSideEffects)) {
            hasSideEffects(derivedAttributes);
        }
        else if (childAttributes.stream().allMatch(IrAttributeUtils::isKnownHasNoSideEffects)) {
            hasNoSideEffects(derivedAttributes);
        }

        return derivedAttributes.buildOrThrow();
    }

    public static Map<AttributeKey, Object> getRepeatabilityAttribute(Map<AttributeKey, Object> childAttributes)
    {
        AttributeKey repeatabilityKey = new AttributeKey(IR, REPEATABILITY);

        return Maps.filterKeys(childAttributes, repeatabilityKey::equals);
    }
}
