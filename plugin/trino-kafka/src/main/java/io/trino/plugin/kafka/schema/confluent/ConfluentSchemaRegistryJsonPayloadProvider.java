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
package io.trino.plugin.kafka.schema.confluent;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.inject.Inject;
import io.trino.decoder.json.JsonPayloadProvider;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class ConfluentSchemaRegistryJsonPayloadProvider
        implements JsonPayloadProvider
{
    private static final int CONFLUENT_PAYLOAD_HEADER_SIZE = 5;
    private static final int MAGIC_BYTE = 0;

    private final JsonMapper mapper;

    @Inject
    public ConfluentSchemaRegistryJsonPayloadProvider(JsonMapper mapper)
    {
        this.mapper = requireNonNull(mapper, "mapper is null");
    }

    @Override
    public JsonNode provide(byte[] data)
            throws IOException
    {
        try (InputStream inputStream = parseBytes(data)) {
            return mapper.readTree(inputStream);
        }
    }

    private static InputStream parseBytes(byte[] data)
    {
        if (data.length < CONFLUENT_PAYLOAD_HEADER_SIZE) {
            throw new IllegalArgumentException("Data length must be at least 5 bytes");
        }

        ByteBuffer buffer = ByteBuffer.wrap(data);
        // https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format
        // Confluent Schema Registry JSON data starts with a byte 0 (byte)
        // followed by a 4-byte schema id (int), then is the actual JSON data
        // for instance, if the data is: "{a:1}" it's encoded as: [123, 97, 58, 49, 125]
        // the schema id is 1, and the first byte is 0,
        // then the full byte array would be: [0, 0, 0, 0, 1, 123, 97, 58, 49, 125]
        byte magicByte = buffer.get();
        verify(magicByte == MAGIC_BYTE, "Invalid MagicByte");
        int schemaId = buffer.getInt();
        verify(schemaId > 0, "Schema id must be greater than 0");
        return new ByteArrayInputStream(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
    }
}
