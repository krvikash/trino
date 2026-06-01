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

import io.trino.server.protocol.OutputColumn;
import io.trino.spi.Page;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.type.ArrayType;
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
import java.util.List;

import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.server.protocol.spooling.encoding.arrow.ArrowSchemaUtils.toArrowSchema;
import static io.trino.spi.type.IntegerType.INTEGER;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.arrow.vector.compression.CompressionUtil.CodecType.NO_COMPRESSION;

@State(Scope.Thread)
@OutputTimeUnit(MILLISECONDS)
@Fork(1)
@Warmup(iterations = 10, time = 1, timeUnit = SECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = SECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkArrowArrayEncoder
{
    @Benchmark
    public int encodeArrayColumn(BenchmarkData data)
            throws IOException
    {
        try (VectorSchemaRoot schemaRoot = VectorSchemaRoot.create(data.schema, data.allocator);
                ArrowPageWriter writer = new ArrowPageWriter(data.columns, schemaRoot, NoCompressionCodec.Factory.INSTANCE, NO_COMPRESSION)) {
            return writer.writePages(OutputStream.nullOutputStream(), List.of(data.page));
        }
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        @Param({"8", "16", "64"})
        private int elementsPerArray;

        @Param({"1024", "8192"})
        private int positionCount;

        private BufferAllocator allocator;
        private List<OutputColumn> columns;
        private Schema schema;
        private Page page;

        @Setup
        public void setup()
        {
            allocator = new RootAllocator();
            ArrayType arrayType = new ArrayType(INTEGER);
            columns = List.of(new OutputColumn(0, "col", arrayType));
            schema = toArrowSchema(columns);

            ArrayBlockBuilder blockBuilder = arrayType.createBlockBuilder(null, positionCount);
            for (int position = 0; position < positionCount; position++) {
                int base = position * elementsPerArray;
                blockBuilder.buildEntry(elements -> {
                    for (int element = 0; element < elementsPerArray; element++) {
                        INTEGER.writeInt(elements, base + element);
                    }
                });
            }
            page = new Page(blockBuilder.build());
        }

        @TearDown(Level.Trial)
        public void tearDown()
        {
            allocator.close();
            allocator = null;
        }
    }

    static void main()
            throws Exception
    {
        benchmark(BenchmarkArrowArrayEncoder.class).run();
    }
}
