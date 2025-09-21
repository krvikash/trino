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
package io.trino.block;

import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.trino.FeaturesConfig;
import io.trino.metadata.BlockEncodingManager;
import io.trino.metadata.InternalBlockEncodingSerde;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.BlockEncoding;
import io.trino.spi.block.BlockEncodingSerde;
import io.trino.tpch.LineItem;
import io.trino.tpch.LineItemGenerator;
import org.junit.jupiter.api.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Arrays;
import java.util.Iterator;
import java.util.stream.IntStream;

import static io.trino.block.BlockAssertions.assertBlockEquals;
import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.util.concurrent.TimeUnit.SECONDS;

@State(Scope.Thread)
@OutputTimeUnit(SECONDS)
@Fork(1)
@Warmup(iterations = 12, time = 1, timeUnit = SECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = SECONDS)
@BenchmarkMode(Mode.Throughput)
public class BenchmarkIntArrayBlockEncoding
{
    @Benchmark
    public void writeBlock(BenchmarkData data, Blackhole blackhole)
    {
        BlockEncodingSerde blockEncodingSerde = data.blockEncodingSerde;
        Block[] blocks = data.blocks;
        BlockEncoding encoding = data.encoding;
        DynamicSliceOutput output = new DynamicSliceOutput(data.maxSerializedBlockSize);

        for (Block block : blocks) {
            encoding.writeBlock(blockEncodingSerde, output, block);
            blackhole.consume(output);
            output.reset();
        }
    }

    @Benchmark
    public void readBlock(BenchmarkData data, Blackhole blackhole)
    {
        BlockEncodingSerde blockEncodingSerde = data.blockEncodingSerde;
        Slice[] serializedBlocks = data.serializedBlocks;
        BlockEncoding encoding = data.encoding;

        for (Slice block : serializedBlocks) {
            blackhole.consume(encoding.readBlock(blockEncodingSerde, block.getInput()));
        }
    }

    @Test
    public void testBenchmarkData()
    {
        for (String dataCharacteristic : new String[] {"random", "delta_friendly", "rle_friendly"}) {
            BenchmarkData data = new BenchmarkData();
            data.dataCharacteristic = dataCharacteristic;
            data.setup();

            for (Block inputBlock : data.blocks) {
                DynamicSliceOutput output = new DynamicSliceOutput(0);
                data.encoding.writeBlock(data.blockEncodingSerde, output, inputBlock);
                Block outputBlock = data.encoding.readBlock(data.blockEncodingSerde, output.slice().getInput());
                assertBlockEquals(INTEGER, inputBlock, outputBlock);
            }
        }
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        private static final int BLOCK_COUNT = 500;
        private static final int POSITIONS_PER_BLOCK = 30_000;

        @Param({"random", "delta_friendly", "rle_friendly"})
        private String dataCharacteristic;

        private BlockEncodingSerde blockEncodingSerde;
        private IntArrayAdaptiveBlockEncoding encoding;
        private Block[] blocks;
        private Slice[] serializedBlocks;
        private int maxSerializedBlockSize;

        @Setup
        public void setup()
        {
            blockEncodingSerde = new InternalBlockEncodingSerde(new BlockEncodingManager(new FeaturesConfig()), TESTING_TYPE_MANAGER);
            encoding = new IntArrayAdaptiveBlockEncoding(true);

            blocks = new Block[BLOCK_COUNT];
            for (int i = 0; i < BLOCK_COUNT; i++) {
                BlockBuilder builder = INTEGER.createFixedSizeBlockBuilder(POSITIONS_PER_BLOCK);
                for (int value : prepareData()) {
                    INTEGER.writeInt(builder, value);
                }
                blocks[i] = builder.build();
            }

            serializedBlocks = new Slice[BLOCK_COUNT];
            for (int i = 0; i < BLOCK_COUNT; i++) {
                DynamicSliceOutput output = new DynamicSliceOutput(0);
                encoding.writeBlock(blockEncodingSerde, output, blocks[i]);
                serializedBlocks[i] = output.slice();
                maxSerializedBlockSize = Math.max(maxSerializedBlockSize, serializedBlocks[i].length());
            }
        }

        private int[] prepareData()
        {
            LineItemGenerator lineItemGenerator = new LineItemGenerator(1, 1, 1);
            Iterator<LineItem> iterator = lineItemGenerator.iterator();

            switch (dataCharacteristic) {
                case "random" -> {
                    return IntStream.range(0, POSITIONS_PER_BLOCK)
                            .map(_ -> iterator.next().shipDate())
                            .toArray();
                }
                case "delta_friendly" -> {
                    int[] values = IntStream.range(0, POSITIONS_PER_BLOCK)
                            .map(_ -> iterator.next().shipDate())
                            .toArray();
                    Arrays.sort(values);
                    return values;
                }
                case "rle_friendly" -> {
                    int[] values = IntStream.range(0, POSITIONS_PER_BLOCK)
                            .map(_ -> iterator.next().lineNumber())
                            .toArray();
                    Arrays.sort(values);
                    return values;
                }
            }
            throw new IllegalArgumentException("Unknown data characteristic: " + dataCharacteristic);
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        benchmark(BenchmarkIntArrayBlockEncoding.class).run();
    }
}
