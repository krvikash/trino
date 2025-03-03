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

import io.trino.client.Column;
import io.trino.client.QueryDataDecoder;
import io.trino.client.spooling.encoding.ArrowQueryDataDecoder;
import io.trino.server.protocol.spooling.QueryDataEncoder;
import io.trino.server.protocol.spooling.encoding.ArrowQueryDataEncoder;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import java.util.List;

public class TestArrowEncodingDecoding
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
    protected QueryDataDecoder createDecoder(List<Column> columns)
    {
        return new ArrowQueryDataDecoder(columns);
    }

    @Override
    protected QueryDataEncoder createEncoder(List<OutputColumn> columns)
    {
        return new ArrowQueryDataEncoder(allocator, CompressionCodec.Factory.INSTANCE, CompressionUtil.CodecType.NO_COMPRESSION, columns);
    }
}
