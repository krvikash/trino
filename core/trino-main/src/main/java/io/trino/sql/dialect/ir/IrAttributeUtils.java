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

import io.trino.sql.newir.Attributes;
import io.trino.sql.newir.Operation.AttributeKey;

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

    public static boolean isKnownDeterministic(Attributes attributes)
    {
        Object repeatability = attributes.get(new AttributeKey(IR, REPEATABILITY));
        return repeatability == DETERMINISTIC;
    }

    public static boolean isKnownNonIdempotent(Attributes attributes)
    {
        Object repeatability = attributes.get(new AttributeKey(IR, REPEATABILITY));
        return repeatability == NON_IDEMPOTENT;
    }

    public static boolean isKnownNonDeterministic(Attributes attributes)
    {
        Object repeatability = attributes.get(new AttributeKey(IR, REPEATABILITY));
        return repeatability == NON_DETERMINISTIC;
    }

    public static boolean isUnknownRepeatability(Attributes attributes)
    {
        Object repeatability = attributes.get(new AttributeKey(IR, REPEATABILITY));
        return repeatability == null;
    }

    public static boolean isKnownSafe(Attributes attributes)
    {
        Object safe = attributes.get(new AttributeKey(IR, SAFE));
        return safe == TRUE;
    }

    public static boolean isKnownHasSideEffects(Attributes attributes)
    {
        Object hasSideEffects = attributes.get(new AttributeKey(IR, HAS_SIDE_EFFECTS));
        return hasSideEffects == TRUE;
    }

    public static boolean isKnownHasNoSideEffects(Attributes attributes)
    {
        Object hasSideEffects = attributes.get(new AttributeKey(IR, HAS_SIDE_EFFECTS));
        return hasSideEffects == FALSE;
    }

    public static void terminalOperation(Attributes.Builder builder)
    {
        builder.putUnchecked(new AttributeKey(IR, IrDialect.TERMINAL), true);
    }

    public static Attributes terminalOperation()
    {
        return Attributes.builder()
                .putUnchecked(new AttributeKey(IR, IrDialect.TERMINAL), true)
                .buildOrThrow();
    }

    public static void deterministic(Attributes.Builder builder)
    {
        builder.putUnchecked(new AttributeKey(IR, REPEATABILITY), DETERMINISTIC);
    }

    public static void nonIdempotent(Attributes.Builder builder)
    {
        builder.putUnchecked(new AttributeKey(IR, REPEATABILITY), NON_IDEMPOTENT);
    }

    public static void nonDeterministic(Attributes.Builder builder)
    {
        builder.putUnchecked(new AttributeKey(IR, REPEATABILITY), NON_DETERMINISTIC);
    }

    public static void safe(Attributes.Builder builder)
    {
        builder.putUnchecked(new AttributeKey(IR, SAFE), true);
    }

    public static void hasSideEffects(Attributes.Builder builder)
    {
        builder.putUnchecked(new AttributeKey(IR, HAS_SIDE_EFFECTS), true);
    }

    public static void hasNoSideEffects(Attributes.Builder builder)
    {
        builder.putUnchecked(new AttributeKey(IR, HAS_SIDE_EFFECTS), false);
    }
}
