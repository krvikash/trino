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
package io.trino.plugin.opensearch.decoders;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.slice.Slices;
import io.trino.plugin.opensearch.DecoderDescriptor;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import org.opensearch.search.SearchHit;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static io.trino.plugin.opensearch.ScanQueryPageSource.getField;
import static io.trino.spi.StandardErrorCode.TYPE_MISMATCH;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class VarcharDecoder
        implements Decoder
{
    private final String path;

    public VarcharDecoder(String path)
    {
        this.path = requireNonNull(path, "path is null");
    }

    @Override
    public void decode(SearchHit hit, Supplier<Object> getter, BlockBuilder output, List<String> dereferenceName)
    {
        Object value = getter.get(); // does to varchar decoder
        if (value == null) {
            output.appendNull();
        }
        else if (value instanceof String || value instanceof Number) {
            VARCHAR.writeSlice(output, Slices.utf8Slice(value.toString()));
        }
        else if (value instanceof Map nestedFields) {
            String nextLevel = dereferenceName.getFirst();
            this.decode(hit, () -> getField(nestedFields, nextLevel), output, dereferenceName.subList(1, dereferenceName.size()));
        }
        else {
            throw new TrinoException(TYPE_MISMATCH, format("Expected a string or numeric value for field '%s' of type VARCHAR: %s [%s]", path, value, value.getClass().getSimpleName()));
        }
    }

    public static class Descriptor
            implements DecoderDescriptor
    {
        private final String path;

        @JsonCreator
        public Descriptor(String path)
        {
            this.path = path;
        }

        @JsonProperty
        public String getPath()
        {
            return path;
        }

        @Override
        public Decoder createDecoder()
        {
            return new VarcharDecoder(path);
        }
    }
}
