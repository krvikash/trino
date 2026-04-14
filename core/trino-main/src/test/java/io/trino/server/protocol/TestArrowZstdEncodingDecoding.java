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
package io.trino.server.protocol;

import io.trino.Session;
import io.trino.client.Column;
import io.trino.client.QueryDataDecoder;
import io.trino.client.spooling.encoding.arrow.ArrowQueryDataDecoder;
import io.trino.server.protocol.spooling.QueryDataEncoder;
import io.trino.server.protocol.spooling.encoding.ArrowCompressionFactory;
import io.trino.server.protocol.spooling.encoding.ArrowQueryDataEncoder;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestArrowZstdEncodingDecoding
        extends AbstractTestEncodingDecoding
{
    private BufferAllocator allocator;

    @BeforeAll
    void setUp()
    {
        allocator = new RootAllocator(1024 * 1024);
    }

    @AfterAll
    public void cleanUp()
    {
        allocator.close();
    }

    @Override
    protected QueryDataDecoder createDecoder(List<Column> columns, boolean supportsVariantBinary)
    {
        return new ArrowQueryDataDecoder(columns);
    }

    @Override
    protected QueryDataEncoder createEncoder(Session session, List<OutputColumn> columns)
    {
        return new ArrowQueryDataEncoder(allocator, new ArrowCompressionFactory(), CompressionUtil.CodecType.ZSTD, columns);
    }

    @Test
    @Override
    public void testVariantSerialization()
    {
        // TODO https://starburstdata.atlassian.net/browse/ENG-9987 Support variant in Trino protocol spooling to Arrow
        assertThatThrownBy(super::testVariantSerialization)
                .hasMessage("Unsupported type: variant");
    }

    @Test
    @Override
    public void testVariantJsonFallbackSerialization()
    {
        // TODO https://starburstdata.atlassian.net/browse/ENG-9987 Support variant in Trino protocol spooling to Arrow
        assertThatThrownBy(super::testVariantJsonFallbackSerialization)
                .hasMessage("Unsupported type: variant");
    }

    @Test
    @Override
    public void testVariantJsonFallbackSerializationInRows()
    {
        // TODO https://starburstdata.atlassian.net/browse/ENG-9987 Support variant in Trino protocol spooling to Arrow
        assertThatThrownBy(super::testVariantJsonFallbackSerializationInRows)
                .hasMessage("Unsupported type: variant");
    }

    @Test
    @Override
    public void testVariantJsonFallbackSerializationInMaps()
    {
        // TODO https://starburstdata.atlassian.net/browse/ENG-9987 Support variant in Trino protocol spooling to Arrow
        assertThatThrownBy(super::testVariantJsonFallbackSerializationInMaps)
                .hasMessage("Unsupported type: variant");
    }

    @Test
    @Override
    public void testVariantBinarySerialization()
    {
        // TODO https://starburstdata.atlassian.net/browse/ENG-9987 Support variant in Trino protocol spooling to Arrow
        assertThatThrownBy(super::testVariantBinarySerialization)
                .hasMessage("Unsupported type: variant");
    }

    @Test
    @Override
    public void testVariantBinarySerializationInRows()
    {
        // TODO https://starburstdata.atlassian.net/browse/ENG-9987 Support variant in Trino protocol spooling to Arrow
        assertThatThrownBy(super::testVariantBinarySerializationInRows)
                .hasMessage("Unsupported type: variant");
    }

    @Test
    @Override
    public void testVariantBinarySerializationInArrays()
    {
        // TODO https://starburstdata.atlassian.net/browse/ENG-9987 Support variant in Trino protocol spooling to Arrow
        assertThatThrownBy(super::testVariantBinarySerializationInArrays)
                .hasMessage("Unsupported type: variant");
    }
}
