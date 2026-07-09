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

import io.airlift.units.DataSize;
import io.trino.server.protocol.OutputColumn;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.VarcharType;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.compression.NoCompressionCodec;
import org.apache.arrow.vector.types.pojo.Schema;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.server.protocol.spooling.encoding.arrow.ArrowSchemaUtils.toArrowSchema;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.arrow.vector.compression.CompressionUtil.CodecType.NO_COMPRESSION;

/**
 * Encodes a page of varchar columns of varying width, the common string-heavy case where the encoder copies the
 * variable-width data and offset buffers.
 */
@State(Scope.Thread)
@OutputTimeUnit(MILLISECONDS)
@Fork(1)
@Warmup(iterations = 10, time = 1, timeUnit = SECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = SECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkArrowVarcharColumnEncoder
{
    @Benchmark
    public int encodeVarcharColumns(BenchmarkData data)
            throws IOException
    {
        try (VectorSchemaRoot schemaRoot = VectorSchemaRoot.create(data.schema, data.allocator);
                ArrowPageWriter writer = new ArrowPageWriter(data.columns, schemaRoot, NoCompressionCodec.Factory.INSTANCE, NO_COMPRESSION, DataSize.of(256, MEGABYTE).toBytes())) {
            return writer.writePages(OutputStream.nullOutputStream(), List.of(data.page));
        }
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        // Short (shipmode), medium (name), long (comment) style columns.
        private static final int[] LENGTHS = {8, 25, 44};

        @Param({"1024", "8192", "65536"})
        private int positionCount;

        private BufferAllocator allocator;
        private List<OutputColumn> columns;
        private Schema schema;
        private Page page;

        @Setup
        public void setup()
        {
            allocator = new RootAllocator();
            List<OutputColumn> outputColumns = new ArrayList<>(LENGTHS.length);
            Block[] blocks = new Block[LENGTHS.length];
            for (int i = 0; i < LENGTHS.length; i++) {
                VarcharType type = VarcharType.createVarcharType(LENGTHS[i]);
                outputColumns.add(new OutputColumn(i, "col" + i, type));
                blocks[i] = buildVarcharBlock(type, positionCount, LENGTHS[i]);
            }
            columns = List.copyOf(outputColumns);
            schema = toArrowSchema(columns);
            page = new Page(blocks);
        }

        @TearDown(Level.Trial)
        public void tearDown()
        {
            allocator.close();
            allocator = null;
        }

        private static Block buildVarcharBlock(VarcharType type, int positionCount, int length)
        {
            BlockBuilder builder = type.createBlockBuilder(null, positionCount);
            for (int position = 0; position < positionCount; position++) {
                StringBuilder value = new StringBuilder(length);
                for (int j = 0; j < length; j++) {
                    value.append((char) ('a' + ((position + j) % 26)));
                }
                type.writeSlice(builder, utf8Slice(value.toString()));
            }
            return builder.build();
        }
    }

    static void main()
            throws Exception
    {
        benchmark(BenchmarkArrowVarcharColumnEncoder.class).run();
    }
}
