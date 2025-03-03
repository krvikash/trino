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
package io.trino.server.protocol.spooling.encoding;

import com.google.inject.Inject;
import io.trino.Session;
import io.trino.client.spooling.DataAttributes;
import io.trino.server.protocol.OutputColumn;
import io.trino.server.protocol.spooling.QueryDataEncoder;
import io.trino.server.protocol.spooling.encoding.arrow.ArrowPageWriter;
import io.trino.server.protocol.spooling.encoding.arrow.ArrowSchemaUtils;
import io.trino.spi.Page;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.arrow.vector.compression.NoCompressionCodec;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import static io.trino.client.spooling.DataAttribute.SEGMENT_SIZE;
import static io.trino.server.protocol.spooling.encoding.arrow.ArrowSchemaUtils.toArrowSchema;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static org.apache.arrow.vector.compression.CompressionUtil.CodecType.NO_COMPRESSION;
import static org.apache.arrow.vector.compression.CompressionUtil.CodecType.ZSTD;

public class ArrowQueryDataEncoder
        implements QueryDataEncoder
{
    private static final String ENCODING = "arrow";

    private final BufferAllocator allocator;
    private final CompressionCodec.Factory compressionFactory;
    private final CompressionUtil.CodecType codecType;
    private final List<OutputColumn> columns;
    private final Schema schema;

    public ArrowQueryDataEncoder(
            BufferAllocator allocator,
            CompressionCodec.Factory compressionFactory,
            CompressionUtil.CodecType codecType,
            List<OutputColumn> columns)
    {
        this.allocator = requireNonNull(allocator, "allocator is null");
        this.compressionFactory = requireNonNull(compressionFactory, "compressionFactory is null");
        this.codecType = requireNonNull(codecType, "codecType is null");
        this.schema = toArrowSchema(columns);
        this.columns = requireNonNull(columns, "columns is null");
    }

    @Override
    public DataAttributes encodeTo(OutputStream output, List<Page> pages)
            throws IOException
    {
        // VectorSchemaRoot can't be shared
        try (VectorSchemaRoot schemaRoot = VectorSchemaRoot.create(schema, allocator)) {
            try (ArrowPageWriter arrowPageWriter = new ArrowPageWriter(columns, schemaRoot, compressionFactory, codecType)) {
                return DataAttributes.builder()
                        .set(SEGMENT_SIZE, toIntExact(arrowPageWriter.writePages(output, pages)))
                        .build();
            }
        }
    }

    @Override
    public void close()
    {
        allocator.close();
    }

    @Override
    public String encoding()
    {
        return switch (codecType) {
            case NO_COMPRESSION -> ENCODING;
            case ZSTD -> ENCODING + "+zstd";
            case LZ4_FRAME -> throw new UnsupportedOperationException("LZ4_FRAME is not supported");
        };
    }

    public static class Factory
            implements QueryDataEncoder.Factory
    {
        private final BufferAllocator allocator;

        @Inject
        public Factory(BufferAllocator rootAllocator)
        {
            this.allocator = requireNonNull(rootAllocator, "allocator is null");
        }

        @Override
        public List<OutputColumn> unsupported(Session session, List<OutputColumn> columns)
        {
            return ArrowSchemaUtils.unsupported(columns);
        }

        @Override
        public QueryDataEncoder create(Session session, List<OutputColumn> columns)
        {
            return new ArrowQueryDataEncoder(
                    allocator.newChildAllocator(session.getQueryId().toString(), Integer.MAX_VALUE, Integer.MAX_VALUE),
                    NoCompressionCodec.Factory.INSTANCE,
                    NO_COMPRESSION,
                    columns);
        }

        @Override
        public String encoding()
        {
            return ENCODING;
        }
    }

    public static class ZstdFactory
            implements QueryDataEncoder.Factory
    {
        private final BufferAllocator allocator;
        private final CompressionCodec.Factory compressionFactory;

        @Inject
        public ZstdFactory(BufferAllocator rootAllocator, CompressionCodec.Factory compressionFactory)
        {
            this.allocator = requireNonNull(rootAllocator, "allocator is null");
            this.compressionFactory = requireNonNull(compressionFactory, "compressionFactory is null");
        }

        @Override
        public QueryDataEncoder create(Session session, List<OutputColumn> columns)
        {
            return new ArrowQueryDataEncoder(
                    allocator.newChildAllocator(session.getQueryId().toString(), Integer.MAX_VALUE, Integer.MAX_VALUE),
                    compressionFactory,
                    ZSTD,
                    columns);
        }

        @Override
        public List<OutputColumn> unsupported(Session session, List<OutputColumn> columns)
        {
            return ArrowSchemaUtils.unsupported(columns);
        }

        @Override
        public String encoding()
        {
            return ENCODING + "+zstd";
        }
    }
}
