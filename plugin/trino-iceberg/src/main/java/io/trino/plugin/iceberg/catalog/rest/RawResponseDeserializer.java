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
package io.trino.plugin.iceberg.catalog.rest;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

import java.io.IOException;
import java.io.StringWriter;

class RawResponseDeserializer
        extends JsonDeserializer<RawResponse>
{
    @Override
    public RawResponse deserialize(JsonParser parser, DeserializationContext context)
            throws IOException
    {
        StringWriter writer = new StringWriter();
        try (JsonGenerator generator = parser.getCodec().getFactory().createGenerator(writer)) {
            generator.copyCurrentStructureExact(parser);
        }
        return new RawResponse(writer.toString());
    }
}
