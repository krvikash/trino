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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.sql.newir.Operation.AttributeKey;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.dialect.ir.IrAttributeDerivationUtils.defaultComposeIrLevelAttributes;
import static io.trino.sql.dialect.ir.IrAttributeDerivationUtils.defaultDeriveFunctionCallIrLevelAttributes;
import static io.trino.sql.dialect.ir.IrAttributeDerivationUtils.defaultDeriveIrLevelAttributes;
import static io.trino.sql.dialect.ir.IrAttributeDerivationUtils.defaultDeriveIrLevelAttributesWithPassthroughSource;
import static io.trino.sql.dialect.ir.IrAttributeDerivationUtils.getRepeatabilityAttribute;
import static io.trino.sql.dialect.ir.IrAttributeDerivationUtils.passIrLevelAttributes;
import static io.trino.sql.dialect.ir.IrDialect.HAS_SIDE_EFFECTS;
import static io.trino.sql.dialect.ir.IrDialect.IR;
import static io.trino.sql.dialect.ir.IrDialect.REPEATABILITY;
import static io.trino.sql.dialect.ir.IrDialect.Repeatability.DETERMINISTIC;
import static io.trino.sql.dialect.ir.IrDialect.Repeatability.NON_DETERMINISTIC;
import static io.trino.sql.dialect.ir.IrDialect.Repeatability.NON_IDEMPOTENT;
import static io.trino.sql.dialect.ir.IrDialect.SAFE;
import static org.assertj.core.api.Assertions.assertThat;

class TestIrAttributeDerivationUtils
{
    private static final Map<AttributeKey, Object> UNKNOWN_ATTRIBUTES = ImmutableMap.of();

    private static final Map<AttributeKey, Object> DETERMINISTIC_ATTRIBUTES = ImmutableMap.of(new AttributeKey(IR, REPEATABILITY), DETERMINISTIC);
    private static final Map<AttributeKey, Object> NON_IDEMPOTENT_ATTRIBUTES = ImmutableMap.of(new AttributeKey(IR, REPEATABILITY), NON_IDEMPOTENT);
    private static final Map<AttributeKey, Object> NON_DETERMINISTIC_ATTRIBUTES = ImmutableMap.of(new AttributeKey(IR, REPEATABILITY), NON_DETERMINISTIC);

    private static final Map<AttributeKey, Object> SAFE_ATTRIBUTES = ImmutableMap.of(new AttributeKey(IR, SAFE), true);

    private static final Map<AttributeKey, Object> HAS_SIDE_EFFECTS_ATTRIBUTES = ImmutableMap.of(new AttributeKey(IR, HAS_SIDE_EFFECTS), true);
    private static final Map<AttributeKey, Object> HAS_NO_SIDE_EFFECTS_ATTRIBUTES = ImmutableMap.of(new AttributeKey(IR, HAS_SIDE_EFFECTS), false);

    private static final Map<AttributeKey, Object> KNOWN_ATTRIBUTES = ImmutableMap.of(
            new AttributeKey(IR, REPEATABILITY), DETERMINISTIC,
            new AttributeKey(IR, SAFE), true,
            new AttributeKey(IR, HAS_SIDE_EFFECTS), false);

    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction RANDOM = FUNCTIONS.resolveFunction("random", fromTypes());
    private static final ResolvedFunction LOWER = FUNCTIONS.resolveFunction("lower", fromTypes(VARCHAR));

    @Test
    public void testDefaultDeriveIrLevelAttributes()
    {
        assertThat(defaultDeriveIrLevelAttributes(ImmutableList.of(DETERMINISTIC_ATTRIBUTES, DETERMINISTIC_ATTRIBUTES)))
                .isEqualTo(DETERMINISTIC_ATTRIBUTES);

        assertThat(defaultDeriveIrLevelAttributes(ImmutableList.of(DETERMINISTIC_ATTRIBUTES, NON_IDEMPOTENT_ATTRIBUTES)))
                .isEqualTo(UNKNOWN_ATTRIBUTES);

        assertThat(defaultDeriveIrLevelAttributes(ImmutableList.of(DETERMINISTIC_ATTRIBUTES, NON_DETERMINISTIC_ATTRIBUTES)))
                .isEqualTo(UNKNOWN_ATTRIBUTES);

        assertThat(defaultDeriveIrLevelAttributes(ImmutableList.of(DETERMINISTIC_ATTRIBUTES, UNKNOWN_ATTRIBUTES)))
                .isEqualTo(UNKNOWN_ATTRIBUTES);

        assertThat(defaultDeriveIrLevelAttributes(ImmutableList.of(SAFE_ATTRIBUTES, SAFE_ATTRIBUTES)))
                .isEqualTo(SAFE_ATTRIBUTES);

        assertThat(defaultDeriveIrLevelAttributes(ImmutableList.of(SAFE_ATTRIBUTES, UNKNOWN_ATTRIBUTES)))
                .isEqualTo(UNKNOWN_ATTRIBUTES);

        assertThat(defaultDeriveIrLevelAttributes(ImmutableList.of(HAS_NO_SIDE_EFFECTS_ATTRIBUTES, HAS_NO_SIDE_EFFECTS_ATTRIBUTES)))
                .isEqualTo(HAS_NO_SIDE_EFFECTS_ATTRIBUTES);

        assertThat(defaultDeriveIrLevelAttributes(ImmutableList.of(HAS_NO_SIDE_EFFECTS_ATTRIBUTES, HAS_SIDE_EFFECTS_ATTRIBUTES)))
                .isEqualTo(HAS_SIDE_EFFECTS_ATTRIBUTES);

        assertThat(defaultDeriveIrLevelAttributes(ImmutableList.of(HAS_NO_SIDE_EFFECTS_ATTRIBUTES, UNKNOWN_ATTRIBUTES)))
                .isEqualTo(UNKNOWN_ATTRIBUTES);

        assertThat(defaultDeriveIrLevelAttributes(ImmutableList.of()))
                .isEqualTo(KNOWN_ATTRIBUTES);
    }

    @Test
    public void testDefaultDeriveFunctionCallIrLevelAttributes()
    {
        assertThat(defaultDeriveFunctionCallIrLevelAttributes(RANDOM, ImmutableList.of(DETERMINISTIC_ATTRIBUTES)))
                .isEqualTo(NON_DETERMINISTIC_ATTRIBUTES);

        assertThat(defaultDeriveFunctionCallIrLevelAttributes(RANDOM, ImmutableList.of(NON_IDEMPOTENT_ATTRIBUTES)))
                .isEqualTo(NON_DETERMINISTIC_ATTRIBUTES);

        assertThat(defaultDeriveFunctionCallIrLevelAttributes(RANDOM, ImmutableList.of(NON_DETERMINISTIC_ATTRIBUTES)))
                .isEqualTo(NON_DETERMINISTIC_ATTRIBUTES);

        assertThat(defaultDeriveFunctionCallIrLevelAttributes(RANDOM, ImmutableList.of(UNKNOWN_ATTRIBUTES)))
                .isEqualTo(NON_DETERMINISTIC_ATTRIBUTES);

        assertThat(defaultDeriveFunctionCallIrLevelAttributes(RANDOM, ImmutableList.of(SAFE_ATTRIBUTES)))
                .isEqualTo(NON_DETERMINISTIC_ATTRIBUTES);

        assertThat(defaultDeriveFunctionCallIrLevelAttributes(RANDOM, ImmutableList.of(HAS_NO_SIDE_EFFECTS_ATTRIBUTES)))
                .isEqualTo(ImmutableMap.of(
                        new AttributeKey(IR, REPEATABILITY), NON_DETERMINISTIC,
                        new AttributeKey(IR, HAS_SIDE_EFFECTS), false));

        assertThat(defaultDeriveFunctionCallIrLevelAttributes(RANDOM, ImmutableList.of(HAS_SIDE_EFFECTS_ATTRIBUTES)))
                .isEqualTo(ImmutableMap.of(
                        new AttributeKey(IR, REPEATABILITY), NON_DETERMINISTIC,
                        new AttributeKey(IR, HAS_SIDE_EFFECTS), true));

        assertThat(defaultDeriveFunctionCallIrLevelAttributes(LOWER, ImmutableList.of(DETERMINISTIC_ATTRIBUTES)))
                .isEqualTo(DETERMINISTIC_ATTRIBUTES);

        assertThat(defaultDeriveFunctionCallIrLevelAttributes(LOWER, ImmutableList.of(NON_IDEMPOTENT_ATTRIBUTES)))
                .isEqualTo(UNKNOWN_ATTRIBUTES);

        assertThat(defaultDeriveFunctionCallIrLevelAttributes(LOWER, ImmutableList.of(NON_DETERMINISTIC_ATTRIBUTES)))
                .isEqualTo(UNKNOWN_ATTRIBUTES);

        assertThat(defaultDeriveFunctionCallIrLevelAttributes(LOWER, ImmutableList.of(UNKNOWN_ATTRIBUTES)))
                .isEqualTo(UNKNOWN_ATTRIBUTES);

        assertThat(defaultDeriveFunctionCallIrLevelAttributes(LOWER, ImmutableList.of(SAFE_ATTRIBUTES)))
                .isEqualTo(UNKNOWN_ATTRIBUTES);

        assertThat(defaultDeriveFunctionCallIrLevelAttributes(LOWER, ImmutableList.of(HAS_NO_SIDE_EFFECTS_ATTRIBUTES)))
                .isEqualTo(HAS_NO_SIDE_EFFECTS_ATTRIBUTES);

        assertThat(defaultDeriveFunctionCallIrLevelAttributes(LOWER, ImmutableList.of(HAS_SIDE_EFFECTS_ATTRIBUTES)))
                .isEqualTo(HAS_SIDE_EFFECTS_ATTRIBUTES);
    }

    @Test
    public void testDefaultComposeFunctionCallIrLevelAttributes()
    {
        assertThat(defaultComposeIrLevelAttributes(ImmutableList.of(DETERMINISTIC_ATTRIBUTES, DETERMINISTIC_ATTRIBUTES)))
                .isEqualTo(DETERMINISTIC_ATTRIBUTES);

        assertThat(defaultComposeIrLevelAttributes(ImmutableList.of(DETERMINISTIC_ATTRIBUTES, NON_IDEMPOTENT_ATTRIBUTES)))
                .isEqualTo(NON_IDEMPOTENT_ATTRIBUTES);

        assertThat(defaultComposeIrLevelAttributes(ImmutableList.of(DETERMINISTIC_ATTRIBUTES, NON_DETERMINISTIC_ATTRIBUTES)))
                .isEqualTo(NON_DETERMINISTIC_ATTRIBUTES);

        assertThat(defaultComposeIrLevelAttributes(ImmutableList.of(DETERMINISTIC_ATTRIBUTES, UNKNOWN_ATTRIBUTES)))
                .isEqualTo(UNKNOWN_ATTRIBUTES);

        assertThat(defaultComposeIrLevelAttributes(ImmutableList.of(NON_IDEMPOTENT_ATTRIBUTES, NON_IDEMPOTENT_ATTRIBUTES)))
                .isEqualTo(NON_IDEMPOTENT_ATTRIBUTES);

        assertThat(defaultComposeIrLevelAttributes(ImmutableList.of(NON_IDEMPOTENT_ATTRIBUTES, NON_DETERMINISTIC_ATTRIBUTES)))
                .isEqualTo(NON_DETERMINISTIC_ATTRIBUTES);

        assertThat(defaultComposeIrLevelAttributes(ImmutableList.of(NON_IDEMPOTENT_ATTRIBUTES, UNKNOWN_ATTRIBUTES)))
                .isEqualTo(UNKNOWN_ATTRIBUTES);

        assertThat(defaultComposeIrLevelAttributes(ImmutableList.of(NON_DETERMINISTIC_ATTRIBUTES, NON_DETERMINISTIC_ATTRIBUTES)))
                .isEqualTo(NON_DETERMINISTIC_ATTRIBUTES);

        assertThat(defaultComposeIrLevelAttributes(ImmutableList.of(NON_DETERMINISTIC_ATTRIBUTES, UNKNOWN_ATTRIBUTES)))
                .isEqualTo(NON_DETERMINISTIC_ATTRIBUTES);

        assertThat(defaultComposeIrLevelAttributes(ImmutableList.of(UNKNOWN_ATTRIBUTES, UNKNOWN_ATTRIBUTES)))
                .isEqualTo(UNKNOWN_ATTRIBUTES);

        assertThat(defaultComposeIrLevelAttributes(ImmutableList.of(SAFE_ATTRIBUTES, SAFE_ATTRIBUTES)))
                .isEqualTo(SAFE_ATTRIBUTES);

        assertThat(defaultComposeIrLevelAttributes(ImmutableList.of(SAFE_ATTRIBUTES, UNKNOWN_ATTRIBUTES)))
                .isEqualTo(UNKNOWN_ATTRIBUTES);

        assertThat(defaultComposeIrLevelAttributes(ImmutableList.of(HAS_NO_SIDE_EFFECTS_ATTRIBUTES, HAS_NO_SIDE_EFFECTS_ATTRIBUTES)))
                .isEqualTo(HAS_NO_SIDE_EFFECTS_ATTRIBUTES);

        assertThat(defaultComposeIrLevelAttributes(ImmutableList.of(HAS_NO_SIDE_EFFECTS_ATTRIBUTES, HAS_SIDE_EFFECTS_ATTRIBUTES)))
                .isEqualTo(HAS_SIDE_EFFECTS_ATTRIBUTES);

        assertThat(defaultComposeIrLevelAttributes(ImmutableList.of(HAS_NO_SIDE_EFFECTS_ATTRIBUTES, UNKNOWN_ATTRIBUTES)))
                .isEqualTo(UNKNOWN_ATTRIBUTES);

        assertThat(defaultComposeIrLevelAttributes(ImmutableList.of(HAS_SIDE_EFFECTS_ATTRIBUTES, HAS_SIDE_EFFECTS_ATTRIBUTES)))
                .isEqualTo(HAS_SIDE_EFFECTS_ATTRIBUTES);

        assertThat(defaultComposeIrLevelAttributes(ImmutableList.of(HAS_SIDE_EFFECTS_ATTRIBUTES, UNKNOWN_ATTRIBUTES)))
                .isEqualTo(HAS_SIDE_EFFECTS_ATTRIBUTES);

        assertThat(defaultComposeIrLevelAttributes(ImmutableList.of()))
                .isEqualTo(KNOWN_ATTRIBUTES);
    }

    @Test
    public void testPassIrLevelAttributes()
    {
        assertThat(passIrLevelAttributes(DETERMINISTIC_ATTRIBUTES))
                .isEqualTo(DETERMINISTIC_ATTRIBUTES);

        assertThat(passIrLevelAttributes(NON_IDEMPOTENT_ATTRIBUTES))
                .isEqualTo(NON_IDEMPOTENT_ATTRIBUTES);

        assertThat(passIrLevelAttributes(NON_DETERMINISTIC_ATTRIBUTES))
                .isEqualTo(NON_DETERMINISTIC_ATTRIBUTES);

        assertThat(passIrLevelAttributes(UNKNOWN_ATTRIBUTES))
                .isEqualTo(UNKNOWN_ATTRIBUTES);

        assertThat(passIrLevelAttributes(SAFE_ATTRIBUTES))
                .isEqualTo(SAFE_ATTRIBUTES);

        assertThat(passIrLevelAttributes(HAS_NO_SIDE_EFFECTS_ATTRIBUTES))
                .isEqualTo(HAS_NO_SIDE_EFFECTS_ATTRIBUTES);

        assertThat(passIrLevelAttributes(HAS_SIDE_EFFECTS_ATTRIBUTES))
                .isEqualTo(HAS_SIDE_EFFECTS_ATTRIBUTES);

        assertThat(passIrLevelAttributes(KNOWN_ATTRIBUTES))
                .isEqualTo(KNOWN_ATTRIBUTES);
    }

    @Test
    public void testDefaultDeriveIrLevelAttributesWithPassthroughSource()
    {
        // passthrough source is deterministic
        assertThat(defaultDeriveIrLevelAttributesWithPassthroughSource(DETERMINISTIC_ATTRIBUTES, ImmutableList.of(DETERMINISTIC_ATTRIBUTES, DETERMINISTIC_ATTRIBUTES)))
                .isEqualTo(DETERMINISTIC_ATTRIBUTES);

        assertThat(defaultDeriveIrLevelAttributesWithPassthroughSource(DETERMINISTIC_ATTRIBUTES, ImmutableList.of(DETERMINISTIC_ATTRIBUTES, NON_IDEMPOTENT_ATTRIBUTES)))
                .isEqualTo(DETERMINISTIC_ATTRIBUTES);

        assertThat(defaultDeriveIrLevelAttributesWithPassthroughSource(DETERMINISTIC_ATTRIBUTES, ImmutableList.of(DETERMINISTIC_ATTRIBUTES, NON_DETERMINISTIC_ATTRIBUTES)))
                .isEqualTo(DETERMINISTIC_ATTRIBUTES);

        assertThat(defaultDeriveIrLevelAttributesWithPassthroughSource(DETERMINISTIC_ATTRIBUTES, ImmutableList.of(DETERMINISTIC_ATTRIBUTES, UNKNOWN_ATTRIBUTES)))
                .isEqualTo(DETERMINISTIC_ATTRIBUTES);

        // passthrough source is non-idempotent
        assertThat(defaultDeriveIrLevelAttributesWithPassthroughSource(NON_IDEMPOTENT_ATTRIBUTES, ImmutableList.of(NON_IDEMPOTENT_ATTRIBUTES, DETERMINISTIC_ATTRIBUTES)))
                .isEqualTo(NON_IDEMPOTENT_ATTRIBUTES);

        assertThat(defaultDeriveIrLevelAttributesWithPassthroughSource(NON_IDEMPOTENT_ATTRIBUTES, ImmutableList.of(NON_IDEMPOTENT_ATTRIBUTES, NON_IDEMPOTENT_ATTRIBUTES)))
                .isEqualTo(NON_IDEMPOTENT_ATTRIBUTES);

        assertThat(defaultDeriveIrLevelAttributesWithPassthroughSource(NON_IDEMPOTENT_ATTRIBUTES, ImmutableList.of(NON_IDEMPOTENT_ATTRIBUTES, NON_DETERMINISTIC_ATTRIBUTES)))
                .isEqualTo(NON_IDEMPOTENT_ATTRIBUTES);

        assertThat(defaultDeriveIrLevelAttributesWithPassthroughSource(NON_IDEMPOTENT_ATTRIBUTES, ImmutableList.of(NON_IDEMPOTENT_ATTRIBUTES, UNKNOWN_ATTRIBUTES)))
                .isEqualTo(NON_IDEMPOTENT_ATTRIBUTES);

        // passthrough source is non-deterministic
        assertThat(defaultDeriveIrLevelAttributesWithPassthroughSource(NON_DETERMINISTIC_ATTRIBUTES, ImmutableList.of(NON_DETERMINISTIC_ATTRIBUTES, DETERMINISTIC_ATTRIBUTES)))
                .isEqualTo(NON_DETERMINISTIC_ATTRIBUTES);

        assertThat(defaultDeriveIrLevelAttributesWithPassthroughSource(NON_DETERMINISTIC_ATTRIBUTES, ImmutableList.of(NON_DETERMINISTIC_ATTRIBUTES, NON_IDEMPOTENT_ATTRIBUTES)))
                .isEqualTo(NON_DETERMINISTIC_ATTRIBUTES);

        assertThat(defaultDeriveIrLevelAttributesWithPassthroughSource(NON_DETERMINISTIC_ATTRIBUTES, ImmutableList.of(NON_DETERMINISTIC_ATTRIBUTES, NON_DETERMINISTIC_ATTRIBUTES)))
                .isEqualTo(NON_DETERMINISTIC_ATTRIBUTES);

        assertThat(defaultDeriveIrLevelAttributesWithPassthroughSource(NON_DETERMINISTIC_ATTRIBUTES, ImmutableList.of(NON_DETERMINISTIC_ATTRIBUTES, UNKNOWN_ATTRIBUTES)))
                .isEqualTo(NON_DETERMINISTIC_ATTRIBUTES);

        // passthrough source is of unknown repeatability
        assertThat(defaultDeriveIrLevelAttributesWithPassthroughSource(UNKNOWN_ATTRIBUTES, ImmutableList.of(UNKNOWN_ATTRIBUTES, DETERMINISTIC_ATTRIBUTES)))
                .isEqualTo(UNKNOWN_ATTRIBUTES);

        assertThat(defaultDeriveIrLevelAttributesWithPassthroughSource(UNKNOWN_ATTRIBUTES, ImmutableList.of(UNKNOWN_ATTRIBUTES, NON_IDEMPOTENT_ATTRIBUTES)))
                .isEqualTo(UNKNOWN_ATTRIBUTES);

        assertThat(defaultDeriveIrLevelAttributesWithPassthroughSource(UNKNOWN_ATTRIBUTES, ImmutableList.of(UNKNOWN_ATTRIBUTES, NON_DETERMINISTIC_ATTRIBUTES)))
                .isEqualTo(UNKNOWN_ATTRIBUTES);

        assertThat(defaultDeriveIrLevelAttributesWithPassthroughSource(UNKNOWN_ATTRIBUTES, ImmutableList.of(UNKNOWN_ATTRIBUTES, UNKNOWN_ATTRIBUTES)))
                .isEqualTo(UNKNOWN_ATTRIBUTES);

        // test safe attribute derivation
        assertThat(defaultDeriveIrLevelAttributesWithPassthroughSource(SAFE_ATTRIBUTES, ImmutableList.of(SAFE_ATTRIBUTES, SAFE_ATTRIBUTES)))
                .isEqualTo(SAFE_ATTRIBUTES);

        assertThat(defaultDeriveIrLevelAttributesWithPassthroughSource(SAFE_ATTRIBUTES, ImmutableList.of(SAFE_ATTRIBUTES, UNKNOWN_ATTRIBUTES)))
                .isEqualTo(UNKNOWN_ATTRIBUTES);

        // test has side effects attribute derivation
        assertThat(defaultDeriveIrLevelAttributesWithPassthroughSource(HAS_NO_SIDE_EFFECTS_ATTRIBUTES, ImmutableList.of(HAS_NO_SIDE_EFFECTS_ATTRIBUTES, HAS_NO_SIDE_EFFECTS_ATTRIBUTES)))
                .isEqualTo(HAS_NO_SIDE_EFFECTS_ATTRIBUTES);

        assertThat(defaultDeriveIrLevelAttributesWithPassthroughSource(HAS_NO_SIDE_EFFECTS_ATTRIBUTES, ImmutableList.of(HAS_NO_SIDE_EFFECTS_ATTRIBUTES, HAS_SIDE_EFFECTS_ATTRIBUTES)))
                .isEqualTo(HAS_SIDE_EFFECTS_ATTRIBUTES);

        assertThat(defaultDeriveIrLevelAttributesWithPassthroughSource(HAS_NO_SIDE_EFFECTS_ATTRIBUTES, ImmutableList.of(HAS_NO_SIDE_EFFECTS_ATTRIBUTES, UNKNOWN_ATTRIBUTES)))
                .isEqualTo(UNKNOWN_ATTRIBUTES);

        assertThat(defaultDeriveIrLevelAttributesWithPassthroughSource(KNOWN_ATTRIBUTES, ImmutableList.of(KNOWN_ATTRIBUTES, UNKNOWN_ATTRIBUTES)))
                .isEqualTo(DETERMINISTIC_ATTRIBUTES);
    }

    @Test
    public void testGetRepeatabilityAttribute()
    {
        assertThat(getRepeatabilityAttribute(DETERMINISTIC_ATTRIBUTES))
                .isEqualTo(DETERMINISTIC_ATTRIBUTES);

        assertThat(getRepeatabilityAttribute(NON_IDEMPOTENT_ATTRIBUTES))
                .isEqualTo(NON_IDEMPOTENT_ATTRIBUTES);

        assertThat(getRepeatabilityAttribute(NON_DETERMINISTIC_ATTRIBUTES))
                .isEqualTo(NON_DETERMINISTIC_ATTRIBUTES);

        assertThat(getRepeatabilityAttribute(UNKNOWN_ATTRIBUTES))
                .isEqualTo(UNKNOWN_ATTRIBUTES);

        assertThat(getRepeatabilityAttribute(KNOWN_ATTRIBUTES))
                .isEqualTo(DETERMINISTIC_ATTRIBUTES);

        assertThat(getRepeatabilityAttribute(SAFE_ATTRIBUTES))
                .isEqualTo(UNKNOWN_ATTRIBUTES);
    }
}
