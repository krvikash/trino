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

import com.google.common.collect.ImmutableSet;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.function.CatalogSchemaFunctionName;
import io.trino.sql.newir.Attributes;
import io.trino.sql.newir.Operation.AttributeKey;

import java.util.List;
import java.util.Set;

import static io.trino.metadata.GlobalFunctionCatalog.isBuiltinFunctionName;
import static io.trino.metadata.OperatorNameUtil.isOperatorName;
import static io.trino.metadata.OperatorNameUtil.unmangleOperator;
import static io.trino.sql.DynamicFilters.isDynamicFilterFunction;
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

    public static Attributes defaultDeriveIrLevelAttributes(List<Attributes> childAttributes)
    {
        Attributes.Builder derivedAttributes = Attributes.builder();

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

    public static Attributes defaultDeriveFunctionCallIrLevelAttributes(ResolvedFunction resolvedFunction, List<Attributes> childAttributes)
    {
        Attributes.Builder derivedAttributes = Attributes.builder();

        if (!resolvedFunction.deterministic()) {
            nonDeterministic(derivedAttributes);
        }
        else if (childAttributes.stream().allMatch(IrAttributeUtils::isKnownDeterministic)) {
            deterministic(derivedAttributes);
        }

        if (isLocallySafe(resolvedFunction) && childAttributes.stream().allMatch(IrAttributeUtils::isKnownSafe)) {
            safe(derivedAttributes);
        }

        if (childAttributes.stream().anyMatch(IrAttributeUtils::isKnownHasSideEffects)) {
            hasSideEffects(derivedAttributes);
        }
        else if (childAttributes.stream().allMatch(IrAttributeUtils::isKnownHasNoSideEffects)) {
            hasNoSideEffects(derivedAttributes);
        }
        // we assume that the function itself has no side effects

        return derivedAttributes.buildOrThrow();
    }

    private static boolean isLocallySafe(ResolvedFunction resolvedFunction)
    {
        if (resolvedFunction.neverFails() || isDynamicFilterFunction(resolvedFunction.name())) {
            return true;
        }

        CatalogSchemaFunctionName functionName = resolvedFunction.name();
        if (!isBuiltinFunctionName(functionName) || !isOperatorName(functionName.functionName())) {
            return false;
        }

        return switch (unmangleOperator(functionName.functionName())) {
            case EQUAL, IDENTICAL, LESS_THAN, LESS_THAN_OR_EQUAL -> true;
            default -> false;
        };
    }

    public static Attributes defaultComposeIrLevelAttributes(List<Attributes> childAttributes)
    {
        Attributes.Builder derivedAttributes = Attributes.builder();

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

    public static Attributes passIrLevelAttributes(Attributes childAttributes)
    {
        Set<AttributeKey> irLevelAttributeKeys = ImmutableSet.of(
                new AttributeKey(IR, REPEATABILITY),
                new AttributeKey(IR, SAFE),
                new AttributeKey(IR, HAS_SIDE_EFFECTS));

        return childAttributes.filterKeys(irLevelAttributeKeys::contains);
    }

    public static Attributes defaultDeriveIrLevelAttributesWithPassthroughSource(Attributes passthroughSourceAttributes, List<Attributes> childAttributes)
    {
        Attributes.Builder derivedAttributes = Attributes.builder();

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

    public static Attributes getRepeatabilityAttribute(Attributes childAttributes)
    {
        AttributeKey repeatabilityKey = new AttributeKey(IR, REPEATABILITY);

        return childAttributes.filterKeys(repeatabilityKey::equals);
    }
}
