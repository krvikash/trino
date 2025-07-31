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
package io.trino.client.spooling.encoding.arrow;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import io.trino.client.CloseableIterator;
import io.trino.client.Column;
import io.trino.client.QueryDataDecoder;
import io.trino.client.spooling.DataAttributes;
import io.trino.client.spooling.encoding.arrow.ArrowDecodingUtils.VectorTypeDecoder;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.util.TransferPair;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

import static io.trino.client.spooling.encoding.arrow.ArrowDecodingUtils.createVectorTypeDecoders;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;

public class ArrowQueryDataDecoder
        implements QueryDataDecoder
{
    private static final String ENCODING = "arrow";

    private static final BufferAllocator ROOT_ALLOCATOR = new RootAllocator(128 * 1024 * 1024);
    private final List<Column> columns;

    public ArrowQueryDataDecoder(List<Column> columns)
    {
        this.columns = ImmutableList.copyOf(columns);
    }

    @Override
    public CloseableIterator<List<Object>> decode(InputStream input, DataAttributes segmentAttributes)
            throws IOException
    {
        BufferAllocator allocator = ROOT_ALLOCATOR.newChildAllocator(randomUUID().toString(), 0, Integer.MAX_VALUE);
        ArrowStreamReader streamReader = new ArrowStreamReader(input, allocator, new ArrowCompressionFactory());
        return new ArrowRowIterator(allocator, streamReader, columns);
    }

    public static class ArrowRowIterator
            extends AbstractIterator<List<Object>>
            implements CloseableIterator<List<Object>>
    {
        private final BufferAllocator allocator;
        private final ArrowReader reader;
        private final List<Column> columns;
        private final VectorSchemaRoot root;

        private final List<List<ValueVector>> batch = new ArrayList<>();
        private VectorTypeDecoder<?>[] currentDecoders;

        private int currentBatch;
        private int currentRowInBatch;
        private int rowsInBatch;

        private boolean loaded;

        public ArrowRowIterator(BufferAllocator allocator, ArrowReader reader, List<Column> columns)
                throws IOException
        {
            this.allocator = requireNonNull(allocator, "allocator is null");
            this.reader = requireNonNull(reader, "reader is null");
            this.columns = requireNonNull(columns, "columns is null");
            this.root = reader.getVectorSchemaRoot();
        }

        private void ensureLoaded()
        {
            if (loaded) {
                return;
            }
            try (ArrowReader ignored = reader; VectorSchemaRoot ignored2 = root) {
                while (reader.loadNextBatch()) {
                    ImmutableList.Builder<ValueVector> vectors = ImmutableList.builderWithExpectedSize(columns.size());
                    for (FieldVector fieldVector : root.getFieldVectors()) {
                        TransferPair t = fieldVector.getTransferPair(allocator);
                        t.transfer();
                        vectors.add(t.getTo());
                    }
                    batch.add(vectors.build());
                }
                root.clear();
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            loaded = true;
            loadBatch(0);
        }

        @Override
        protected List<Object> computeNext()
        {
            if (currentBatch == 0 && currentRowInBatch == 0) {
                ensureLoaded();
            }

            if (currentRowInBatch >= rowsInBatch) {
                currentBatch++;
                if (!loadBatch(currentBatch)) {
                    return endOfData();
                }
            }
            return decodeNextRow();
        }

        @Override
        public String toString()
        {
            return "ArrowRowIterator{reader=" + reader + '}';
        }

        public List<Object> decodeNextRow()
        {
            ArrayList<Object> row = new ArrayList<>();
            for (VectorTypeDecoder<?> vectorDecoder : currentDecoders) {
                row.add(vectorDecoder.decode(currentRowInBatch));
            }
            currentRowInBatch++;
            return row;
        }

        @Override
        public void close()
                throws IOException
        {
            unloadAllBatches();
            reader.close();
            allocator.close();
        }

        private boolean loadBatch(int batchIndex)
        {
            if (batchIndex < 0 || batchIndex >= batch.size()) {
                return false;
            }
            unloadBatch(batchIndex - 1);
            currentBatch = batchIndex;
            currentDecoders = createVectorTypeDecoders(columns, batch.get(batchIndex));
            rowsInBatch = batch.get(batchIndex).get(0).getValueCount();
            currentRowInBatch = 0;
            return true;
        }

        private void unloadBatch(int batchIndex)
        {
            if (batchIndex < 0 || batchIndex >= batch.size()) {
                return;
            }
            for (ValueVector vector : batch.get(batchIndex)) {
                vector.clear();
                vector.close();
            }
        }

        private void unloadAllBatches()
        {
            for (List<ValueVector> vectors : batch) {
                vectors.forEach(ValueVector::clear);
                vectors.forEach(ValueVector::close);
            }
            batch.clear();
        }
    }

    public static class Factory
            implements QueryDataDecoder.Factory
    {
        @Override
        public QueryDataDecoder create(List<Column> columns, DataAttributes queryAttributes)
        {
            return new ArrowQueryDataDecoder(columns);
        }

        @Override
        public String encoding()
        {
            return ENCODING;
        }

        @Override
        public boolean isPreferred()
        {
            return false; // still experimental, so not preferred
        }
    }

    private static class ZstdArrowQueryDataDecoder
            extends ArrowQueryDataDecoder
    {
        public ZstdArrowQueryDataDecoder(List<Column> columns)
        {
            super(columns);
        }

        @Override
        public String encoding()
        {
            return super.encoding() + "+zstd";
        }
    }

    // Arrow knows internally how to decode Zstd, so we don't need to do anything special here
    public static class ZstdFactory
            extends Factory
    {
        @Override
        public QueryDataDecoder create(List<Column> columns, DataAttributes queryAttributes)
        {
            return new ZstdArrowQueryDataDecoder(columns);
        }

        @Override
        public String encoding()
        {
            return super.encoding() + "+zstd";
        }
    }

    @Override
    public String encoding()
    {
        return ENCODING;
    }
}
