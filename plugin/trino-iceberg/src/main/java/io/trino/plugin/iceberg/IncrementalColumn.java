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
package io.trino.plugin.iceberg;

import io.trino.spi.type.DecimalType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.VarcharType;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types.NestedField;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.iceberg.IcebergTypes.convertIcebergValueToTrino;
import static io.trino.plugin.iceberg.TypeConverter.toTrinoType;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static java.util.Objects.requireNonNull;

record IncrementalColumn(String name, NestedField field, Type trinoType)
{
    IncrementalColumn
    {
        requireNonNull(name, "name is null");
        requireNonNull(field, "field is null");
        requireNonNull(trinoType, "trinoType is null");
    }

    static Optional<IncrementalColumn> findWithExactUpperBound(Schema storageSchema, String columnName, TypeManager typeManager)
    {
        NestedField field = storageSchema.caseInsensitiveFindField(columnName);
        if (field == null) {
            return Optional.empty();
        }
        Type trinoType = toTrinoType(field.type(), typeManager);
        if (!hasExactIcebergUpperBound(trinoType)) {
            return Optional.empty();
        }
        return Optional.of(new IncrementalColumn(columnName, field, trinoType));
    }

    /**
     * Any other type is stored as VARCHAR by
     * {@code AbstractTrinoCatalog#typeForMaterializedViewStorageTable}, so the {@code SELECT max(col)}
     * subquery would compare a VARCHAR against the source column's own type.
     */
    static boolean isSupportedIncrementalColumnType(Type type)
    {
        return type instanceof VarcharType || hasExactIcebergUpperBound(type);
    }

    /**
     * Iceberg truncates and rounds VARCHAR and binary upper bounds <em>up</em>, so a bound for those can
     * exceed the true maximum and silently drop source rows. Every type admitted here must also stay
     * renderable by {@code IncrementalColumnWatermarkLiteral}.
     */
    static boolean hasExactIcebergUpperBound(Type type)
    {
        if (type.equals(INTEGER) || type.equals(BIGINT) || type.equals(DATE) || type instanceof DecimalType) {
            return true;
        }
        if (type instanceof TimeType timeType) {
            return timeType.getPrecision() <= TIME_MICROS.getPrecision();
        }
        if (type instanceof TimestampType timestampType) {
            return timestampType.getPrecision() <= TIMESTAMP_MICROS.getPrecision();
        }
        return false;
    }

    /**
     * ORC tracks timestamp statistics at millisecond granularity, so {@code OrcMetrics#timestampMinMax}
     * pads the upper bound out to the end of its millisecond. A watermark set on such a bound skips
     * every source row in that window.
     */
    boolean hasExactUpperBoundIn(IcebergFileFormat fileFormat)
    {
        return !(trinoType instanceof TimestampType) || fileFormat != IcebergFileFormat.ORC;
    }

    Optional<Object> maxValueIn(Metrics fileMetrics)
    {
        Map<Integer, ByteBuffer> upperBounds = fileMetrics.upperBounds();
        ByteBuffer upperBound = upperBounds == null ? null : upperBounds.get(field.fieldId());
        if (upperBound == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(convertIcebergValueToTrino(field.type(), Conversions.fromByteBuffer(field.type(), upperBound)));
    }

    boolean isNullInEveryRowOf(Metrics fileMetrics)
    {
        Map<Integer, Long> nullValueCounts = fileMetrics.nullValueCounts();
        Long nullValueCount = nullValueCounts == null ? null : nullValueCounts.get(field.fieldId());
        return nullValueCount != null && nullValueCount.equals(fileMetrics.recordCount());
    }
}
