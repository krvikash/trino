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
package io.trino.server.protocol.spooling.encoding.arrow;

import com.google.common.collect.ImmutableList;
import io.trino.server.protocol.OutputColumn;
import io.trino.spi.Page;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.UuidType;
import io.trino.spi.type.VarcharType;
import io.trino.type.IpAddressType;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.IntervalDayVector;
import org.apache.arrow.vector.IntervalYearVector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil.CodecType;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.message.IpcOption;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.server.protocol.spooling.encoding.arrow.ArrowWriter.checkedCast;
import static java.lang.Math.toIntExact;
import static java.nio.channels.Channels.newChannel;
import static java.util.Objects.requireNonNull;

public class ArrowPageWriter
        implements AutoCloseable
{
    private final VectorSchemaRoot schema;
    private final List<ArrowWriter> vectorWriters;
    private final List<Integer> sourceChannels;
    private final CompressionCodec.Factory compressionFactory;
    private final CodecType codecType;

    public ArrowPageWriter(List<OutputColumn> columns, VectorSchemaRoot schema, CompressionCodec.Factory compressionFactory, CodecType codecType)
    {
        this.schema = requireNonNull(schema, "schema is null");
        this.vectorWriters = createVectorWriters(schema, columns);
        this.sourceChannels = columns.stream()
                .map(OutputColumn::sourcePageChannel)
                .collect(toImmutableList());
        this.compressionFactory = requireNonNull(compressionFactory, "compressionFactory is null");
        this.codecType = requireNonNull(codecType, "codecType is null");
    }

    public int writePages(OutputStream outputStream, List<Page> pages)
            throws IOException
    {
        try (ArrowStreamWriter streamWriter = new ArrowStreamWriter(schema, null, newChannel(outputStream), IpcOption.DEFAULT, compressionFactory, codecType)) {
            for (Page page : pages) {
                schema.setRowCount(page.getPositionCount());
                for (int i = 0; i < vectorWriters.size(); i++) {
                    vectorWriters.get(i).initialize(page.getBlock(sourceChannels.get(i)));
                    vectorWriters.get(i).write(page.getBlock(sourceChannels.get(i)));
                }
                streamWriter.writeBatch();
            }
            return toIntExact(streamWriter.bytesWritten());
        }
    }

    private static List<ArrowWriter> createVectorWriters(VectorSchemaRoot schema, List<OutputColumn> columns)
    {
        ImmutableList.Builder<ArrowWriter> writers = ImmutableList.builderWithExpectedSize(columns.size());
        for (int i = 0; i < columns.size(); i++) {
            writers.add(writerForVector(schema.getVector(i), columns.get(i).type()));
        }
        return writers.build();
    }

    @Override
    public void close()
            throws IOException
    {
        schema.clear();
        schema.close();
    }

    public static ArrowWriter writerForVector(ValueVector valueVector, Type type)
    {
        return switch (valueVector) {
            case NullVector vector -> new NullWriter(vector);
            case BitVector vector -> new BooleanWriter(vector);
            case TinyIntVector vector -> new TinyIntWriter(vector);
            case SmallIntVector vector -> new SmallIntWriter(vector);
            case IntVector vector -> new IntegerWriter(vector);
            case BigIntVector vector -> new BigintWriter(vector);
            case Float4Vector vector -> new RealWriter(vector);
            case Float8Vector vector -> new DoubleWriter(vector);
            case VarBinaryVector vector -> new VarbinaryWriter(vector);
            case DateDayVector vector -> new DateWriter(vector);
            case IntervalDayVector vector -> new IntervalDayWriter(vector);
            case IntervalYearVector vector -> new IntervalYearMonthWriter(vector);
            case TimeSecVector vector -> new TimeSecWriter(vector);
            case TimeMilliVector vector -> new TimeMilliWriter(vector);
            case TimeMicroVector vector -> new TimeMicroWriter(vector);
            case TimeNanoVector vector -> new TimeNanoWriter(vector);
            case TimeStampSecVector vector -> new TimestampSecWriter(vector);
            case TimeStampMilliVector vector -> new TimestampMilliWriter(vector);
            case TimeStampMicroVector vector -> new TimestampMicroWriter(vector);
            case TimeStampNanoVector vector -> new TimestampNanoWriter(vector);

            // Type dependant writers
            case DecimalVector vector when type instanceof DecimalType decimalType ->
                    new DecimalWriter(vector, decimalType);
            case VarCharVector vector when type instanceof CharType charType ->
                    new CharWriter(vector, charType);
            case VarCharVector vector when type instanceof VarcharType ->
                    new VarcharWriter(vector);
            case FixedSizeBinaryVector vector when type instanceof UuidType ->
                    new UuidWriter(vector);
            case FixedSizeBinaryVector vector when type instanceof IpAddressType ->
                    new IpAddressWriter(vector);
            case MapVector vector when type instanceof MapType mapType ->
                    writerForVector(vector, mapType);
            case ListVector vector when type instanceof ArrayType arrayType ->
                    new ArrayWriter(vector, writerForVector(vector.getDataVector(), arrayType.getElementType()));
            case StructVector structVector when type instanceof RowType rowType ->
                    new RowWriter(structVector, writersForVectors(structVector.getChildrenFromFields(), rowType.getFields()));
            case StructVector structVector when type instanceof TimeWithTimeZoneType timeWithTimeZoneType ->
                    new TimeWithTimeZoneWriter(structVector, timeWithTimeZoneType.getPrecision());
            case StructVector structVector when type instanceof TimestampWithTimeZoneType timestampWithTimeZoneType ->
                    new TimestampWithTimeZoneWriter(structVector, timestampWithTimeZoneType.getPrecision());
            default -> throw unsupportedVectorException(valueVector, type);
        };
    }

    private static List<ArrowWriter> writersForVectors(List<FieldVector> vectors, List<RowType.Field> fields)
    {
        verify(vectors.size() == fields.size(), "List of vectors and row fields have different length");
        ImmutableList.Builder<ArrowWriter> childWriters = ImmutableList.builderWithExpectedSize(fields.size());
        for (int i = 0; i < vectors.size(); i++) {
            childWriters.add(writerForVector(vectors.get(i), fields.get(i).getType()));
        }
        return childWriters.build();
    }

    private static MapWriter writerForVector(MapVector mapVector, MapType mapType)
    {
        if (!(mapVector.getDataVector() instanceof StructVector structVector)) {
            throw new UnsupportedOperationException("Expected map vector to be a struct for type %s but got %s".formatted(mapType, mapVector.getDataVector().getClass()));
        }
        return new MapWriter(
                mapVector,
                writerForVector(checkedCast(structVector.getChild("key"), FieldVector.class), mapType.getKeyType()),
                writerForVector(checkedCast(structVector.getChild("value"), FieldVector.class), mapType.getValueType()));
    }

    private static UnsupportedOperationException unsupportedVectorException(ValueVector vector, Type type)
    {
        return new UnsupportedOperationException("Unsupported vector %s for type %s".formatted(vector.getClass().getSimpleName(), type));
    }
}
