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
package io.trino.plugin.elasticsearch.decoders;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.plugin.elasticsearch.DecoderDescriptor;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.TimestampType;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.search.SearchHit;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.TYPE_MISMATCH;
import static io.trino.spi.type.TimestampType.MAX_SHORT_PRECISION;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.Timestamps.round;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;

public class TimestampDecoder
        implements Decoder
{
    private final String path;
    private final DateFormatter formatter;
    private final int precision;

    public TimestampDecoder(String path, List<String> formats, int precision)
    {
        this.path = requireNonNull(path, "path is null");
        checkState(precision >= 0, "precision must be non-negative");
        this.precision = precision;
        if (formats.isEmpty()) {
            formatter = DateFormatter.forPattern("strict_date_optional_time||epoch_millis");
        }
        else {
            formatter = DateFormatter.forPattern(String.join("||", formats));
        }
    }

    @Override
    public void decode(SearchHit hit, Supplier<Object> getter, BlockBuilder output)
    {
        DocumentField documentField = hit.getFields().get(path);
        Object value;

        if (documentField != null) {
            if (documentField.getValues().size() > 1) {
                throw new TrinoException(TYPE_MISMATCH, format("Expected single value for column '%s', found: %s", path, documentField.getValues().size()));
            }
            value = documentField.getValue();
        }
        else {
            value = getter.get();
        }

        if (value == null) {
            output.appendNull();
            return;
        }

        if (value instanceof Number) {
            value = String.valueOf(value);
        }

        if (value instanceof String valueString) {
            if (precision <= 3) {
                long epochMicros = formatter.parseMillis(valueString) * MICROSECONDS_PER_MILLISECOND;
                TIMESTAMP_MILLIS.writeLong(output, epochMicros);
            }
            else {
                Instant instant = parseInstant(valueString);
                if (precision > MAX_SHORT_PRECISION) {
                    long epochMicros = (instant.getEpochSecond() * MICROSECONDS_PER_SECOND) + (instant.getNano() / NANOSECONDS_PER_MICROSECOND);
                    int picosOfMicro = (instant.getNano() % NANOSECONDS_PER_MICROSECOND) * PICOSECONDS_PER_NANOSECOND;
                    verify(picosOfMicro == round(picosOfMicro, TimestampType.MAX_PRECISION - precision),
                            "Invalid value of picosOfMicro for precision %s: %s", precision, picosOfMicro);
                    TIMESTAMP_NANOS.writeObject(output, new LongTimestamp(epochMicros, picosOfMicro));
                }
            }
        }
        else {
            // Elasticsearch currently only support type 'date' and 'date_nanos' for date field, precision <= MAX_SHORT_PRECISION is unsupported.
            throw new TrinoException(NOT_SUPPORTED, format(
                    "Unsupported representation for field '%s' of type TIMESTAMP: %s [%s]",
                    path,
                    value,
                    value.getClass().getSimpleName()));
        }
    }

    private Instant parseInstant(String value)
    {
        TemporalAccessor temporalAccessor = formatter.parse(value);

        if (temporalAccessor.isSupported(ChronoField.INSTANT_SECONDS)) {
            return Instant.from(temporalAccessor);
        }

        // If no timezone is present, interpret the date/time as UTC
        LocalDateTime localDateTime = LocalDateTime.of(
                temporalAccessor.get(ChronoField.YEAR_OF_ERA),
                temporalAccessor.get(ChronoField.MONTH_OF_YEAR),
                temporalAccessor.get(ChronoField.DAY_OF_MONTH),
                temporalAccessor.get(ChronoField.HOUR_OF_DAY),
                temporalAccessor.get(ChronoField.MINUTE_OF_HOUR),
                temporalAccessor.get(ChronoField.SECOND_OF_MINUTE),
                temporalAccessor.get(ChronoField.NANO_OF_SECOND));
        return localDateTime.toInstant(UTC);
    }

    public static class Descriptor
            implements DecoderDescriptor
    {
        private final String path;
        private final List<String> formats;
        private final int precision;

        @JsonCreator
        public Descriptor(String path, List<String> formats, int precision)
        {
            this.path = path;
            this.formats = ImmutableList.copyOf(formats);
            this.precision = precision;
        }

        @JsonProperty
        public String getPath()
        {
            return path;
        }

        @JsonProperty
        public List<String> getFormats()
        {
            return formats;
        }

        @JsonProperty
        public int getPrecision()
        {
            return precision;
        }

        @Override
        public Decoder createDecoder()
        {
            return new TimestampDecoder(path, formats, precision);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Descriptor that = (Descriptor) o;
            return Objects.equals(this.path, that.path);
        }

        @Override
        public int hashCode()
        {
            return path.hashCode();
        }
    }
}
