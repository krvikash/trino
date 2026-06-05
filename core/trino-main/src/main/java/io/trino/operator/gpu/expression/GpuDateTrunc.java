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
package io.trino.operator.gpu.expression;

import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.DType;
import ai.rapids.cudf.Scalar;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.base.gpu.ClosingOnce;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;

public class GpuDateTrunc
        implements GpuExpression
{
    private final GpuExpression argument;
    private final Field field;

    public GpuDateTrunc(GpuExpression argument, Field field)
    {
        this.argument = requireNonNull(argument, "argument is null");
        this.field = requireNonNull(field, "field is null");
    }

    @Override
    public @Move ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputColumns)
    {
        try (ClosingOnce<ColumnVector> timestamp = ClosingOnce.own(argument.evaluate(positionCount, inputColumns))) {
            DType columnType = timestamp.borrow().getType();
            OptionalLong period = field.periodInNativeUnits(columnType);
            if (period.isEmpty()) {
                // If the field is finer than the column's native precision, the floor is a no-op.
                return timestamp.borrow().incRefCount();
            }
            try (ClosingOnce<ColumnVector> asInt64 = ClosingOnce.own(timestamp.borrow().castTo(DType.INT64))) {
                timestamp.close();
                // cuDF dateTimeFloor produces incorrect results for large-magnitude negative timestamps.
                // Cast to INT64 for arithmetic (pmod doesn't accept timestamp LHS), compute
                // floor(x, p) = x - pmod(x, p), then cast back to the original timestamp type.
                try (Scalar periodScalar = Scalar.fromLong(period.getAsLong());
                        ClosingOnce<ColumnVector> floorMod = ClosingOnce.own(asInt64.borrow().pmod(periodScalar));
                        ColumnVector floored = asInt64.borrow().sub(floorMod.borrow())) {
                    floorMod.close();
                    return floored.castTo(columnType);
                }
            }
        }
    }

    public enum Field
    {
        MILLISECOND("millisecond"),
        SECOND("second"),
        MINUTE("minute"),
        HOUR("hour"),
        DAY("day");
        // week, month, quarter, year are not supported because they have variable length

        private static final Map<String, Field> BY_UNIT;

        static {
            ImmutableMap.Builder<String, Field> builder = ImmutableMap.builder();
            for (Field field : values()) {
                builder.put(field.trinoDateTruncUnit, field);
            }
            BY_UNIT = builder.buildOrThrow();
        }

        private final String trinoDateTruncUnit;

        Field(String trinoDateTruncUnit)
        {
            this.trinoDateTruncUnit = requireNonNull(trinoDateTruncUnit, "unit is null");
        }

        public String trinoDateTruncUnit()
        {
            return trinoDateTruncUnit;
        }

        /**
         * Returns the floor period in the column's native units, or empty if the field is
         * finer than the column's precision (making the floor a no-op).
         */
        public OptionalLong periodInNativeUnits(DType columnType)
        {
            if (columnType.equals(DType.TIMESTAMP_SECONDS)) {
                return switch (this) {
                    case MILLISECOND, SECOND -> OptionalLong.empty(); // no sub-second digits
                    case MINUTE -> OptionalLong.of(60);
                    case HOUR -> OptionalLong.of(3_600);
                    case DAY -> OptionalLong.of(86_400);
                };
            }
            if (columnType.equals(DType.TIMESTAMP_MILLISECONDS)) {
                return switch (this) {
                    case MILLISECOND -> OptionalLong.empty(); // already millisecond-aligned
                    case SECOND -> OptionalLong.of(1_000);
                    case MINUTE -> OptionalLong.of(60_000);
                    case HOUR -> OptionalLong.of(3_600_000);
                    case DAY -> OptionalLong.of(86_400_000);
                };
            }
            if (columnType.equals(DType.TIMESTAMP_MICROSECONDS)) {
                return switch (this) {
                    case MILLISECOND -> OptionalLong.of(1_000);
                    case SECOND -> OptionalLong.of(1_000_000);
                    case MINUTE -> OptionalLong.of(60_000_000);
                    case HOUR -> OptionalLong.of(3_600_000_000L);
                    case DAY -> OptionalLong.of(86_400_000_000L);
                };
            }
            if (columnType.equals(DType.TIMESTAMP_NANOSECONDS)) {
                return switch (this) {
                    case MILLISECOND -> OptionalLong.of(1_000_000);
                    case SECOND -> OptionalLong.of(1_000_000_000L);
                    case MINUTE -> OptionalLong.of(60_000_000_000L);
                    case HOUR -> OptionalLong.of(3_600_000_000_000L);
                    case DAY -> OptionalLong.of(86_400_000_000_000L);
                };
            }
            throw new IllegalArgumentException("Unsupported column type: " + columnType);
        }

        public static Optional<Field> forTrinoDateTruncUnit(String trinoDateTruncUnit)
        {
            return Optional.ofNullable(BY_UNIT.get(trinoDateTruncUnit));
        }
    }
}
