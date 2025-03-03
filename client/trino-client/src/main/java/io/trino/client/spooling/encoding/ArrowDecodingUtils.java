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
package io.trino.client.spooling.encoding;

import com.google.common.collect.ImmutableMap;
import com.google.common.net.InetAddresses;
import io.trino.client.ClientTypeSignature;
import io.trino.client.ClientTypeSignatureParameter;
import io.trino.client.Column;
import io.trino.client.NamedClientTypeSignature;
import io.trino.client.Row;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.IntervalDayVector;
import org.apache.arrow.vector.IntervalYearVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteOrder;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.trino.client.ClientStandardTypes.ARRAY;
import static io.trino.client.ClientStandardTypes.BIGINT;
import static io.trino.client.ClientStandardTypes.BING_TILE;
import static io.trino.client.ClientStandardTypes.BOOLEAN;
import static io.trino.client.ClientStandardTypes.CHAR;
import static io.trino.client.ClientStandardTypes.COLOR;
import static io.trino.client.ClientStandardTypes.DATE;
import static io.trino.client.ClientStandardTypes.DECIMAL;
import static io.trino.client.ClientStandardTypes.DOUBLE;
import static io.trino.client.ClientStandardTypes.GEOMETRY;
import static io.trino.client.ClientStandardTypes.HYPER_LOG_LOG;
import static io.trino.client.ClientStandardTypes.INTEGER;
import static io.trino.client.ClientStandardTypes.INTERVAL_DAY_TO_SECOND;
import static io.trino.client.ClientStandardTypes.INTERVAL_YEAR_TO_MONTH;
import static io.trino.client.ClientStandardTypes.IPADDRESS;
import static io.trino.client.ClientStandardTypes.JSON;
import static io.trino.client.ClientStandardTypes.KDB_TREE;
import static io.trino.client.ClientStandardTypes.MAP;
import static io.trino.client.ClientStandardTypes.P4_HYPER_LOG_LOG;
import static io.trino.client.ClientStandardTypes.QDIGEST;
import static io.trino.client.ClientStandardTypes.REAL;
import static io.trino.client.ClientStandardTypes.ROW;
import static io.trino.client.ClientStandardTypes.SET_DIGEST;
import static io.trino.client.ClientStandardTypes.SMALLINT;
import static io.trino.client.ClientStandardTypes.SPHERICAL_GEOGRAPHY;
import static io.trino.client.ClientStandardTypes.TIME;
import static io.trino.client.ClientStandardTypes.TIMESTAMP;
import static io.trino.client.ClientStandardTypes.TIMESTAMP_WITH_TIME_ZONE;
import static io.trino.client.ClientStandardTypes.TIME_WITH_TIME_ZONE;
import static io.trino.client.ClientStandardTypes.TINYINT;
import static io.trino.client.ClientStandardTypes.UUID;
import static io.trino.client.ClientStandardTypes.VARCHAR;
import static io.trino.client.IntervalDayTime.formatMillis;
import static io.trino.client.IntervalYearMonth.formatMonths;
import static io.trino.client.spooling.encoding.ArrowDateTimeUtils.MICROSECONDS_PER_MILLISECOND;
import static io.trino.client.spooling.encoding.ArrowDateTimeUtils.NANOSECONDS_PER_MICROSECOND;
import static io.trino.client.spooling.encoding.ArrowDateTimeUtils.NANOSECONDS_PER_MILLISECOND;
import static io.trino.client.spooling.encoding.ArrowDateTimeUtils.PICOSECONDS_PER_MICROSECOND;
import static io.trino.client.spooling.encoding.ArrowDateTimeUtils.PICOSECONDS_PER_MILLISECOND;
import static io.trino.client.spooling.encoding.ArrowDateTimeUtils.PICOSECONDS_PER_NANOSECOND;
import static io.trino.client.spooling.encoding.ArrowDateTimeUtils.PICOSECONDS_PER_SECOND;
import static io.trino.client.spooling.encoding.ArrowDateTimeUtils.PRECISION_MICROS;
import static io.trino.client.spooling.encoding.ArrowDateTimeUtils.PRECISION_MILLIS;
import static io.trino.client.spooling.encoding.ArrowDateTimeUtils.PRECISION_NANOS;
import static io.trino.client.spooling.encoding.ArrowDateTimeUtils.PRECISION_SECONDS;
import static io.trino.client.spooling.encoding.ArrowDateTimeUtils.formatOffset;
import static io.trino.client.spooling.encoding.ArrowDateTimeUtils.formatTime;
import static io.trino.client.spooling.encoding.ArrowDateTimeUtils.formatTimestamp;
import static io.trino.client.spooling.encoding.ArrowDateTimeUtils.formatTimestampWithTimeZone;
import static java.lang.Long.reverseBytes;
import static java.lang.Math.floorDiv;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;

public class ArrowDecodingUtils
{
    private ArrowDecodingUtils()
    {
    }

    public static VectorTypeDecoder<?>[] createVectorTypeDecoders(List<Column> columns, List<ValueVector> vectors)
    {
        verify(!columns.isEmpty(), "Columns can not be empty");
        verify(!vectors.isEmpty(), "Vectors can not be empty");
        verify(columns.size() == vectors.size(), "Columns and vectors must have the same size");

        VectorTypeDecoder<?>[] decoders = new VectorTypeDecoder<?>[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            decoders[i] = createVectorTypeDecoder(columns.get(i).getTypeSignature(), vectors.get(i));
        }
        return decoders;
    }

    private static VectorTypeDecoder<?> createVectorTypeDecoder(ClientTypeSignature signature, ValueVector vector)
    {
        switch (signature.getRawType()) {
            case VARCHAR:
                return new VarcharDecoder(checkedCast(vector, VarCharVector.class));
            case CHAR:
                return new CharDecoder(signature.getArguments().get(0).getLongLiteral(), checkedCast(vector, VarCharVector.class));
            case MAP:
                return new MapDecoder(signature, checkedCast(vector, MapVector.class));
            case ARRAY:
                return new ArrayDecoder(signature, checkedCast(vector, ListVector.class));
            case DATE:
                return new DateDecoder(checkedCast(vector, DateDayVector.class));
            case UUID:
                return new UuidDecoder(checkedCast(vector, FixedSizeBinaryVector.class));
            case IPADDRESS:
                return new IpAddressDecoder(checkedCast(vector, FixedSizeBinaryVector.class));
            case DECIMAL:
                return new DecimalDecoder(checkedCast(vector, DecimalVector.class));
            case INTERVAL_DAY_TO_SECOND:
                return new IntervalDaySecondDecoder(checkedCast(vector, IntervalDayVector.class));
            case INTERVAL_YEAR_TO_MONTH:
                return new IntervalYearMonthDecoder(checkedCast(vector, IntervalYearVector.class));
            case ROW:
                return new RowDecoder(signature, checkedCast(vector, StructVector.class));
            case TIME: {
                long precision = signature.getArguments().get(0).getLongLiteral();
                if (precision == PRECISION_SECONDS) {
                    return new TimeSecDecoder(checkedCast(vector, TimeSecVector.class));
                }
                if (precision == PRECISION_MILLIS) {
                    return new TimeMilliDecoder(checkedCast(vector, TimeMilliVector.class));
                }
                if (precision == PRECISION_MICROS) {
                    return new TimeMicroDecoder(checkedCast(vector, TimeMicroVector.class));
                }
                if (precision == PRECISION_NANOS) {
                    return new TimeNanoDecoder(checkedCast(vector, TimeNanoVector.class));
                }

                throw new UnsupportedOperationException(format("Unsupported time(%d) type", precision));
            }
            case TIMESTAMP_WITH_TIME_ZONE: {
                long precision = signature.getArguments().get(0).getLongLiteral();
                if (precision == PRECISION_SECONDS) {
                    return new TimestampSecWithTimeZoneDecoder(checkedCast(vector, StructVector.class));
                }
                if (precision == PRECISION_MILLIS) {
                    return new TimestampMilliWithTimeZoneDecoder(checkedCast(vector, StructVector.class));
                }
                // TODO: support 6 to 9 precisions
                throw new UnsupportedOperationException(format("Unsupported timestamp(%d) with time zone type", precision));
            }
            case TIME_WITH_TIME_ZONE: {
                long precision = signature.getArguments().get(0).getLongLiteral();
                if (precision == PRECISION_SECONDS) {
                    return new TimeSecWithTimeZoneDecoder(checkedCast(vector, StructVector.class));
                }
                if (precision == PRECISION_MILLIS) {
                    return new TimeMilliWithTimeZoneDecoder(checkedCast(vector, StructVector.class));
                }
                // TODO: Support precisions from 6 to 9
                throw new UnsupportedOperationException(format("Unsupported time(%d) with timezone type", precision));
            }
            case TIMESTAMP: {
                long precision = signature.getArguments().get(0).getLongLiteral();
                if (precision == PRECISION_SECONDS) {
                    return new TimestampSecDecoder(checkedCast(vector, TimeStampSecVector.class));
                }
                if (precision == PRECISION_MILLIS) {
                    return new TimestampMilliDecoder(checkedCast(vector, TimeStampMilliVector.class));
                }
                if (precision == PRECISION_MICROS) {
                    return new TimestampMicroDecoder(checkedCast(vector, TimeStampMicroVector.class));
                }
                if (precision == PRECISION_NANOS) {
                    return new TimestampNanoDecoder(checkedCast(vector, TimeStampNanoVector.class));
                }
                throw new UnsupportedOperationException(format("Unsupported timestamp(%d) type", precision));
            }
            case JSON:
            case GEOMETRY:
            case SPHERICAL_GEOGRAPHY:
            case COLOR:
            case KDB_TREE:
            case BING_TILE:
            case QDIGEST:
            case P4_HYPER_LOG_LOG:
            case HYPER_LOG_LOG:
            case SET_DIGEST:
            case BIGINT:
            case BOOLEAN:
            case INTEGER:
            case SMALLINT:
            case TINYINT:
            case DOUBLE:
            case REAL:
            default:
                return new PassThroughDecoder(vector);
        }
    }

    private static class PassThroughDecoder
            implements VectorTypeDecoder<Object>
    {
        private final ValueVector vector;

        public PassThroughDecoder(ValueVector vector)
        {
            this.vector = requireNonNull(vector, "vector is null");
        }

        @Override
        public Object decode(int position)
        {
            if (vector.isNull(position)) {
                return null;
            }
            return vector.getObject(position);
        }

        @Override
        public void close()
        {
            vector.close();
        }
    }

    private static class VarcharDecoder
            implements VectorTypeDecoder<String>
    {
        private final VarCharVector vector;

        public VarcharDecoder(VarCharVector vector)
        {
            this.vector = requireNonNull(vector, "vector is null");
        }

        @Override
        public String decode(int position)
        {
            if (vector.isNull(position)) {
                return null;
            }
            return new String(vector.get(position), UTF_8);
        }

        @Override
        public void close()
        {
            vector.close();
        }
    }

    private static class CharDecoder
            implements VectorTypeDecoder
    {
        private final long length;
        private final VarCharVector vector;

        public CharDecoder(long length, VarCharVector vector)
        {
            this.length = length;
            this.vector = requireNonNull(vector, "vector is null");
        }

        @Override
        public Object decode(int position)
        {
            if (vector.isNull(position)) {
                return null;
            }
            String value = vector.getObject(position).toString();
            int codePoints = value.codePointCount(0, value.length());
            if (codePoints < length) {
                return value + " ".repeat(toIntExact(length - codePoints));
            }
            return value;
        }

        @Override
        public void close()
        {
            vector.close();
        }
    }

    private static class MapDecoder
            implements VectorTypeDecoder<Map<Object, Object>>
    {
        private final VectorTypeDecoder<?> keyDecoder;
        private final VectorTypeDecoder<?> valueDecoder;
        private final MapVector vector;

        public MapDecoder(ClientTypeSignature signature, MapVector vector)
        {
            requireNonNull(signature, "signature is null");
            this.vector = requireNonNull(vector, "vector is null");
            StructVector structVector = (StructVector) vector.getDataVector();

            checkArgument(signature.getRawType().equals(MAP), "not a map type signature: %s", signature);
            this.keyDecoder = createVectorTypeDecoder(signature.getArgumentsAsTypeSignatures().get(0), structVector.getChild("key"));
            this.valueDecoder = createVectorTypeDecoder(signature.getArgumentsAsTypeSignatures().get(1), structVector.getChild("value"));
        }

        @Override
        public Map<Object, Object> decode(int position)
        {
            if (vector.isNull(position)) {
                return null;
            }

            if (vector.isEmpty(position)) {
                return emptyMap();
            }

            Map<Object, Object> values = new HashMap<>();
            for (int i = vector.getElementStartIndex(position); i < vector.getElementEndIndex(position); i++) {
                values.put(keyDecoder.decode(i), valueDecoder.decode(i));
            }
            return unmodifiableMap(values);
        }

        @Override
        public void close()
                throws IOException
        {
            keyDecoder.close();
            valueDecoder.close();
            vector.close();
        }
    }

    private static class RowDecoder
            implements VectorTypeDecoder<Row>
    {
        private final StructVector vector;
        private final Map<NamedClientTypeSignature, VectorTypeDecoder<?>> fieldDecoders;

        public RowDecoder(ClientTypeSignature signature, StructVector vector)
        {
            requireNonNull(signature, "signature is null");
            this.vector = requireNonNull(vector, "vector is null");

            List<ClientTypeSignatureParameter> arguments = signature.getArguments();
            ImmutableMap.Builder<NamedClientTypeSignature, VectorTypeDecoder<?>> builder = ImmutableMap.builderWithExpectedSize(arguments.size());
            for (int i = 0; i < arguments.size(); i++) {
                NamedClientTypeSignature namedTypeSignature = arguments.get(i).getNamedTypeSignature();
                builder.put(
                        namedTypeSignature,
                        createVectorTypeDecoder(namedTypeSignature.getTypeSignature(), vector.getChildByOrdinal(i)));
            }
            this.fieldDecoders = builder.buildOrThrow();
        }

        @Override
        public Row decode(int position)
        {
            if (vector.isNull(position)) {
                return null;
            }

            Row.Builder builder = Row.builderWithExpectedSize(fieldDecoders.size());
            fieldDecoders.forEach((name, decoder) -> {
                if (name.getFieldName().isPresent()) {
                    builder.addField(name.getFieldName().orElseThrow().getName(), decoder.decode(position));
                }
                else {
                    builder.addUnnamedField(decoder.decode(position));
                }
            });

            return builder.build();
        }

        @Override
        public void close()
                throws IOException
        {
            for (VectorTypeDecoder<?> vectorTypeDecoder : fieldDecoders.values()) {
                vectorTypeDecoder.close();
            }
            vector.close();
        }
    }

    private static class ArrayDecoder
            implements VectorTypeDecoder<List<?>>
    {
        private final VectorTypeDecoder<?> valueDecoder;
        private final ListVector vector;

        public ArrayDecoder(ClientTypeSignature signature, ListVector vector)
        {
            requireNonNull(signature, "signature is null");
            checkArgument(signature.getRawType().equals(ARRAY), "Expected array type signature but got: %s", signature);
            this.vector = requireNonNull(vector, "vector is null");
            this.valueDecoder = createVectorTypeDecoder(signature.getArgumentsAsTypeSignatures().get(0), vector.getDataVector());
        }

        @Override
        public List<?> decode(int position)
        {
            if (vector.isNull(position)) {
                return null;
            }

            if (vector.isEmpty(position)) {
                return emptyList();
            }

            List<Object> values = new ArrayList<>();
            for (int i = vector.getElementStartIndex(position); i < vector.getElementEndIndex(position); i++) {
                values.add(valueDecoder.decode(i));
            }
            return unmodifiableList(values);
        }

        @Override
        public void close()
                throws IOException
        {
            valueDecoder.close();
            vector.close();
        }
    }

    private static class DateDecoder
            implements VectorTypeDecoder<String>
    {
        private final DateDayVector vector;

        public DateDecoder(DateDayVector vector)
        {
            this.vector = requireNonNull(vector, "vector is null");
        }

        @Override
        public String decode(int position)
        {
            if (vector.isNull(position)) {
                return null;
            }
            return LocalDate.ofEpochDay(vector.get(position)).toString();
        }

        @Override
        public void close()
        {
            vector.close();
        }
    }

    private static class TimestampSecDecoder
            implements VectorTypeDecoder<String>
    {
        private final TimeStampSecVector vector;

        public TimestampSecDecoder(TimeStampSecVector vector)
        {
            this.vector = requireNonNull(vector, "vector is null");
        }

        @Override
        public String decode(int position)
        {
            if (vector.isNull(position)) {
                return null;
            }

            return formatTimestamp(PRECISION_SECONDS, vector.get(position) * NANOSECONDS_PER_MILLISECOND, 0);
        }

        @Override
        public void close()
        {
            vector.close();
        }
    }

    private static class TimestampSecWithTimeZoneDecoder
            implements VectorTypeDecoder<String>
    {
        private final StructVector vector;
        private final TimeStampSecVector timestampSecVector;
        private final VarCharVector timezoneVector;

        public TimestampSecWithTimeZoneDecoder(StructVector vector)
        {
            this.vector = requireNonNull(vector, "vector is null");
            this.timestampSecVector = checkedCast(vector.getChild("timestamp"), TimeStampSecVector.class);
            this.timezoneVector = checkedCast(vector.getChild("timezone"), VarCharVector.class);
        }

        @Override
        public String decode(int position)
        {
            if (vector.isNull(position)) {
                return null;
            }
            return formatTimestampWithTimeZone(PRECISION_SECONDS, timestampSecVector.get(position) * MICROSECONDS_PER_MILLISECOND, 0, ZoneId.of(timezoneVector.getObject(position).toString()));
        }

        @Override
        public void close()
        {
            vector.close();
        }
    }

    private static class TimestampMilliWithTimeZoneDecoder
            implements VectorTypeDecoder<String>
    {
        private final StructVector vector;
        private final TimeStampMilliVector timeStampMilliVector;
        private final VarCharVector timezoneVector;

        public TimestampMilliWithTimeZoneDecoder(StructVector vector)
        {
            this.vector = requireNonNull(vector, "vector is null");
            this.timeStampMilliVector = checkedCast(vector.getChild("timestamp"), TimeStampMilliVector.class);
            this.timezoneVector = checkedCast(vector.getChild("timezone"), VarCharVector.class);
        }

        @Override
        public String decode(int position)
        {
            if (vector.isNull(position)) {
                return null;
            }
            return formatTimestampWithTimeZone(PRECISION_SECONDS, timeStampMilliVector.get(position), 3, ZoneId.of(timezoneVector.getObject(position).toString()));
        }

        @Override
        public void close()
        {
            vector.close();
        }
    }

    private static class TimestampMilliDecoder
            implements VectorTypeDecoder<String>
    {
        private final TimeStampMilliVector vector;

        public TimestampMilliDecoder(TimeStampMilliVector vector)
        {
            this.vector = requireNonNull(vector, "vector is null");
        }

        @Override
        public String decode(int position)
        {
            if (vector.isNull(position)) {
                return null;
            }

            return formatTimestamp(PRECISION_MILLIS, vector.get(position) * NANOSECONDS_PER_MICROSECOND, 0);
        }

        @Override
        public void close()
        {
            vector.close();
        }
    }

    private static class TimestampMicroDecoder
            implements VectorTypeDecoder<String>
    {
        private final TimeStampMicroVector vector;

        public TimestampMicroDecoder(TimeStampMicroVector vector)
        {
            this.vector = requireNonNull(vector, "vector is null");
        }

        @Override
        public String decode(int position)
        {
            if (vector.isNull(position)) {
                return null;
            }

            return formatTimestamp(PRECISION_MICROS, vector.get(position), 0);
        }

        @Override
        public void close()
        {
            vector.close();
        }
    }

    private static class TimestampNanoDecoder
            implements VectorTypeDecoder<String>
    {
        private final TimeStampNanoVector vector;

        public TimestampNanoDecoder(TimeStampNanoVector vector)
        {
            this.vector = requireNonNull(vector, "vector is null");
        }

        @Override
        public String decode(int position)
        {
            if (vector.isNull(position)) {
                return null;
            }

            long nanos = vector.get(position);
            long micros = floorDiv(nanos, NANOSECONDS_PER_MICROSECOND);
            int picos = toIntExact(nanos - micros * NANOSECONDS_PER_MICROSECOND) * PICOSECONDS_PER_NANOSECOND;

            return formatTimestamp(PRECISION_NANOS, micros, picos);
        }

        @Override
        public void close()
        {
            vector.close();
        }
    }

    private static class DecimalDecoder
            implements VectorTypeDecoder<String>
    {
        private final DecimalVector vector;

        public DecimalDecoder(DecimalVector vector)
        {
            this.vector = requireNonNull(vector, "vector is null");
        }

        @Override
        public String decode(int position)
        {
            if (vector.isNull(position)) {
                return null;
            }
            return vector.getObject(position).toString();
        }

        @Override
        public void close()
        {
            vector.close();
        }
    }

    private static class TimeSecDecoder
            implements VectorTypeDecoder<String>
    {
        private final TimeSecVector vector;

        public TimeSecDecoder(TimeSecVector vector)
        {
            this.vector = requireNonNull(vector, "vector is null");
        }

        @Override
        public String decode(int position)
        {
            if (vector.isNull(position)) {
                return null;
            }

            return formatTime(PRECISION_SECONDS, vector.get(position) * PICOSECONDS_PER_SECOND);
        }

        @Override
        public void close()
        {
            vector.close();
        }
    }

    private static class TimeSecWithTimeZoneDecoder
            implements VectorTypeDecoder<String>
    {
        private final StructVector vector;
        private final TimeSecVector timeSecVector;
        private final IntVector offsetVector;

        public TimeSecWithTimeZoneDecoder(StructVector vector)
        {
            this.vector = requireNonNull(vector, "vector is null");
            this.timeSecVector = checkedCast(vector.getChild("time"), TimeSecVector.class);
            this.offsetVector = checkedCast(vector.getChild("offset"), IntVector.class);
        }

        @Override
        public String decode(int position)
        {
            if (vector.isNull(position)) {
                return null;
            }
            return formatTime(PRECISION_SECONDS, timeSecVector.get(position) * PICOSECONDS_PER_SECOND) + formatOffset(offsetVector.get(position));
        }

        @Override
        public void close()
        {
            vector.close();
        }
    }

    private static class TimeMilliWithTimeZoneDecoder
            implements VectorTypeDecoder<String>
    {
        private final StructVector vector;
        private final TimeMilliVector timeMilliVector;
        private final IntVector offsetVector;

        public TimeMilliWithTimeZoneDecoder(StructVector vector)
        {
            this.vector = requireNonNull(vector, "vector is null");
            this.timeMilliVector = checkedCast(vector.getChild("time"), TimeMilliVector.class);
            this.offsetVector = checkedCast(vector.getChild("offset"), IntVector.class);
        }

        @Override
        public String decode(int position)
        {
            if (vector.isNull(position)) {
                return null;
            }
            return formatTime(PRECISION_MILLIS, (long) timeMilliVector.get(position) * PICOSECONDS_PER_MILLISECOND) + formatOffset(offsetVector.get(position));
        }

        @Override
        public void close()
        {
            vector.close();
        }
    }

    private static class TimeMilliDecoder
            implements VectorTypeDecoder<String>
    {
        private final TimeMilliVector vector;

        public TimeMilliDecoder(TimeMilliVector vector)
        {
            this.vector = requireNonNull(vector, "vector is null");
        }

        @Override
        public String decode(int position)
        {
            if (vector.isNull(position)) {
                return null;
            }

            return formatTime(PRECISION_MILLIS, (long) vector.get(position) * PICOSECONDS_PER_MILLISECOND);
        }

        @Override
        public void close()
        {
            vector.close();
        }
    }

    private static class TimeMicroDecoder
            implements VectorTypeDecoder<String>
    {
        private final TimeMicroVector vector;

        public TimeMicroDecoder(TimeMicroVector vector)
        {
            this.vector = requireNonNull(vector, "vector is null");
        }

        @Override
        public String decode(int position)
        {
            if (vector.isNull(position)) {
                return null;
            }

            return formatTime(PRECISION_MICROS, vector.get(position) * PICOSECONDS_PER_MICROSECOND);
        }

        @Override
        public void close()
        {
            vector.close();
        }
    }

    private static class TimeNanoDecoder
            implements VectorTypeDecoder<String>
    {
        private final TimeNanoVector vector;

        public TimeNanoDecoder(TimeNanoVector vector)
        {
            this.vector = requireNonNull(vector, "vector is null");
        }

        @Override
        public String decode(int position)
        {
            if (vector.isNull(position)) {
                return null;
            }

            return formatTime(PRECISION_NANOS, vector.get(position) * PICOSECONDS_PER_NANOSECOND);
        }

        @Override
        public void close()
        {
            vector.close();
        }
    }

    private static class UuidDecoder
            implements VectorTypeDecoder<String>
    {
        private static final VarHandle BIG_ENDIAN_LONG_VIEW = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);

        private final FixedSizeBinaryVector vector;

        public UuidDecoder(FixedSizeBinaryVector vector)
        {
            this.vector = requireNonNull(vector, "vector is null");
        }

        @Override
        public String decode(int position)
        {
            if (vector.isNull(position)) {
                return null;
            }
            byte[] bytes = vector.get(position);
            return new UUID(reverseBytes((long) BIG_ENDIAN_LONG_VIEW.get(bytes, 0)), reverseBytes((long) BIG_ENDIAN_LONG_VIEW.get(bytes, 8))).toString();
        }

        @Override
        public void close()
        {
            vector.close();
        }
    }

    private static class IpAddressDecoder
            implements VectorTypeDecoder<String>
    {
        private static final VarHandle BIG_ENDIAN_LONG_VIEW = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);

        private final FixedSizeBinaryVector vector;

        public IpAddressDecoder(FixedSizeBinaryVector vector)
        {
            this.vector = requireNonNull(vector, "vector is null");
        }

        @Override
        public String decode(int position)
        {
            if (vector.isNull(position)) {
                return null;
            }

            byte[] bytes = vector.get(position);
            byte[] swapped = new byte[16];

            BIG_ENDIAN_LONG_VIEW.set(swapped, 0, reverseBytes((long) BIG_ENDIAN_LONG_VIEW.get(bytes, 0)));
            BIG_ENDIAN_LONG_VIEW.set(swapped, 8, reverseBytes((long) BIG_ENDIAN_LONG_VIEW.get(bytes, 8)));

            try {
                return InetAddresses.toAddrString(InetAddress.getByAddress(swapped));
            }
            catch (UnknownHostException e) {
                throw new IllegalArgumentException();
            }
        }

        @Override
        public void close()
        {
            vector.close();
        }
    }

    private static class IntervalDaySecondDecoder
            implements VectorTypeDecoder<String>
    {
        private final IntervalDayVector vector;

        public IntervalDaySecondDecoder(IntervalDayVector vector)
        {
            this.vector = requireNonNull(vector, "vector is null");
        }

        @Override
        public String decode(int position)
        {
            if (vector.isNull(position)) {
                return null;
            }
            return formatMillis(vector.getObject(position).toMillis());
        }

        @Override
        public void close()
        {
            vector.close();
        }
    }

    private static class IntervalYearMonthDecoder
            implements VectorTypeDecoder<String>
    {
        private final IntervalYearVector vector;

        public IntervalYearMonthDecoder(IntervalYearVector vector)
        {
            this.vector = requireNonNull(vector, "vector is null");
        }

        @Override
        public String decode(int position)
        {
            if (vector.isNull(position)) {
                return null;
            }
            return formatMonths(vector.get(position));
        }

        @Override
        public void close()
        {
            vector.close();
        }
    }

    public interface VectorTypeDecoder<T>
            extends Closeable
    {
        T decode(int position);
    }

    private static <T extends FieldVector> T checkedCast(ValueVector vector, Class<T> clazz)
    {
        requireNonNull(vector, "vector is null");
        checkArgument(clazz.isInstance(vector), "Expected %s, but got %s", clazz, vector.getClass());
        return clazz.cast(vector);
    }
}
