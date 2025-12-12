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
package io.trino.execution.buffer;

import com.google.inject.Inject;
import io.trino.spi.PageStreamFactory;
import io.trino.spi.PageStreamReader;
import io.trino.spi.PageStreamWriter;
import io.trino.spi.block.BlockEncodingSerde;

import javax.crypto.SecretKey;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Optional;

import static io.trino.execution.buffer.PagesSerdes.createSpillingPagesSerdeFactory;

public class PagesSerdeStreamFactory
        implements PageStreamFactory
{
    private static final Optional<SecretKey> ENCRYPTION_KEY = Optional.empty();

    private final PagesSerdeFactory serdeFactory;

    @Inject
    public PagesSerdeStreamFactory(BlockEncodingSerde blockEncodingSerde)
    {
        serdeFactory = createSpillingPagesSerdeFactory(blockEncodingSerde, CompressionCodec.LZ4);
    }

    @Override
    public PageStreamReader createReader(InputStream inputStream)
    {
        return new PagesSerdeStreamReader(inputStream, serdeFactory.createDeserializer(ENCRYPTION_KEY));
    }

    @Override
    public PageStreamWriter createWriter(OutputStream outputStream)
    {
        return new PagesSerdeStreamWriter(outputStream, serdeFactory.createSerializer(ENCRYPTION_KEY));
    }
}
