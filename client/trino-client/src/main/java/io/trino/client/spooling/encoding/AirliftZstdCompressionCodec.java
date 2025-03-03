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

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.compression.AbstractCompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;

import java.nio.ByteBuffer;

import static io.trino.client.spooling.encoding.DecompressionUtils.decompressZstd;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static org.apache.arrow.vector.compression.CompressionUtil.CodecType.ZSTD;

public class AirliftZstdCompressionCodec
        extends AbstractCompressionCodec
{
    @Override
    protected ArrowBuf doCompress(BufferAllocator allocator, ArrowBuf uncompressedBuffer)
    {
        throw new UnsupportedOperationException("Compression is not supported on the client side");
    }

    @Override
    protected ArrowBuf doDecompress(BufferAllocator allocator, ArrowBuf compressedBuffer)
    {
        int uncompressedLength = toIntExact(readUncompressedLength(compressedBuffer));
        ArrowBuf uncompressedBuffer = allocator.buffer(uncompressedLength);

        ByteBuffer inputBuffer = compressedBuffer.nioBuffer(8L, toIntExact(compressedBuffer.writerIndex() - 8L));
        ByteBuffer outputBuffer = uncompressedBuffer.nioBuffer(0, uncompressedLength);

        long actualUncompressedLength = decompressZstd(inputBuffer, outputBuffer);

        if (actualUncompressedLength != uncompressedLength) {
            uncompressedBuffer.close();
            throw new RuntimeException(format("Expected to uncompressed %d bytes but actual was %d", uncompressedLength, actualUncompressedLength));
        }
        return uncompressedBuffer;
    }

    @Override
    public CompressionUtil.CodecType getCodecType()
    {
        return ZSTD;
    }
}
