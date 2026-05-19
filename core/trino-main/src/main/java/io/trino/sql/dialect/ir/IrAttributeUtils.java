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
import io.trino.sql.newir.Operation.AttributeKey;

import java.util.Map;

import static io.trino.sql.dialect.ir.IrDialect.HAS_SIDE_EFFECTS;
import static io.trino.sql.dialect.ir.IrDialect.IR;
import static io.trino.sql.dialect.ir.IrDialect.REPEATABILITY;
import static io.trino.sql.dialect.ir.IrDialect.Repeatability.DETERMINISTIC;
import static io.trino.sql.dialect.ir.IrDialect.Repeatability.NON_DETERMINISTIC;
import static io.trino.sql.dialect.ir.IrDialect.Repeatability.NON_IDEMPOTENT;
import static io.trino.sql.dialect.ir.IrDialect.SAFE;
import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;

public class IrAttributeUtils
{
    private IrAttributeUtils() {}

    public static boolean isKnownDeterministic(Map<AttributeKey, Object> attributes)
    {
        Object repeatability = attributes.get(new AttributeKey(IR, REPEATABILITY));
        return repeatability == DETERMINISTIC;
    }

    public static boolean isKnownNonIdempotent(Map<AttributeKey, Object> attributes)
    {
        Object repeatability = attributes.get(new AttributeKey(IR, REPEATABILITY));
        return repeatability == NON_IDEMPOTENT;
    }

    public static boolean isKnownNonDeterministic(Map<AttributeKey, Object> attributes)
    {
        Object repeatability = attributes.get(new AttributeKey(IR, REPEATABILITY));
        return repeatability == NON_DETERMINISTIC;
    }

    public static boolean isUnknownRepeatability(Map<AttributeKey, Object> attributes)
    {
        Object repeatability = attributes.get(new AttributeKey(IR, REPEATABILITY));
        return repeatability == null;
    }

    public static boolean isKnownSafe(Map<AttributeKey, Object> attributes)
    {
        Object safe = attributes.get(new AttributeKey(IR, SAFE));
        return safe == TRUE;
    }

    public static boolean isKnownHasSideEffects(Map<AttributeKey, Object> attributes)
    {
        Object hasSideEffects = attributes.get(new AttributeKey(IR, HAS_SIDE_EFFECTS));
        return hasSideEffects == TRUE;
    }

    public static boolean isKnownHasNoSideEffects(Map<AttributeKey, Object> attributes)
    {
        Object hasSideEffects = attributes.get(new AttributeKey(IR, HAS_SIDE_EFFECTS));
        return hasSideEffects == FALSE;
    }

    public static void terminalOperation(ImmutableMap.Builder<AttributeKey, Object> builder)
    {
        builder.put(new AttributeKey(IR, IrDialect.TERMINAL), true);
    }

    public static Map<AttributeKey, Object> terminalOperation()
    {
        return ImmutableMap.of(new AttributeKey(IR, IrDialect.TERMINAL), true);
    }

    public static void deterministic(ImmutableMap.Builder<AttributeKey, Object> builder)
    {
        builder.put(new AttributeKey(IR, REPEATABILITY), DETERMINISTIC);
    }

    public static void nonIdempotent(ImmutableMap.Builder<AttributeKey, Object> builder)
    {
        builder.put(new AttributeKey(IR, REPEATABILITY), NON_IDEMPOTENT);
    }

    public static void nonDeterministic(ImmutableMap.Builder<AttributeKey, Object> builder)
    {
        builder.put(new AttributeKey(IR, REPEATABILITY), NON_DETERMINISTIC);
    }

    public static void safe(ImmutableMap.Builder<AttributeKey, Object> builder)
    {
        builder.put(new AttributeKey(IR, SAFE), true);
    }

    public static void hasSideEffects(ImmutableMap.Builder<AttributeKey, Object> builder)
    {
        builder.put(new AttributeKey(IR, HAS_SIDE_EFFECTS), true);
    }

    public static void hasNoSideEffects(ImmutableMap.Builder<AttributeKey, Object> builder)
    {
        builder.put(new AttributeKey(IR, HAS_SIDE_EFFECTS), false);
    }
}
